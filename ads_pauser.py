import os
os.environ['ROCKETADS_CONFIG_FILE'] = '/Users/yao.jiacheng/Documents/rocketads/settings.yml'

import pandas as pd
import numpy as np

import matplotlib.pyplot as plt

from rocketads.adwords.hierarchy import get_mcc_hierarchy_raw
from rocketads.adwords.reports import get_report_for_accounts
from rocketads.oauth2.credentials import get_default_production_client
from rocketads.adwords.account_operations import batch_add_labels, retrieve_labels
from rocketads.adwords.account_operations import partial_failure_process_chunks, partial_failure_mutate_label_chunks, \
    batch_add_labels_placeholders, batch_add_labels_to_keywords, initialize_label_placeholders
from rocketads.utils.currency_conversion import get_exchange_rate, add_fx_rate_column

from fastbetabino import posterior_cdf, posterior_sf

from plt_save import save


def get_update_operations(df):
    operations = [{
        'operator': 'SET',
        'operand': {
            'xsi_type': 'BiddableAdGroupCriterion',
            'adGroupId': str(row['ad_group_id']),
            'criterion': {
                'id': str(row['keyword_id']),
            },
            'userStatus': 'PAUSED'
        }
    } for _, row in df.iterrows()]

    return operations


def get_label_operations(df, label_id):
    operations = [{
        'operator': 'ADD',
        'operand': {
            'xsi_type': 'AdGroupCriterionLabel ',
            'adGroupId': str(row['ad_group_id']),
            'criterionId': str(row['keyword_id']),
            'labelId': str(label_id)
        }
    } for _, row in df.iterrows()]

    return operations


def _ad_pauser_decide_keywords_to_pause(client):

    graph = get_mcc_hierarchy_raw(client, False)
    all_accounts = [(customer.name, customer.customerId) for customer in graph.entries]

    query = """
            SELECT  Id, AdGroupId, CampaignId, CampaignName, AccountDescriptiveName, AdGroupName, Conversions,
            KeywordMatchType, Cost, AccountCurrencyCode
            FROM KEYWORDS_PERFORMANCE_REPORT
            WHERE CampaignStatus=ENABLED AND AdGroupStatus=ENABLED AND Status=ENABLED AND
                  Conversions < 10 AND Clicks > 1
            DURING 20160909,20161009
            """
    df1 = get_report_for_accounts(query, [el[1] for el in all_accounts], n_jobs=10)
    query = """
                SELECT  KeywordTextMatchingQuery, KeywordId, AdGroupId, CampaignId, OrganicAveragePosition, OrganicClicks,
                OrganicImpressions, Clicks, SearchQuery
                FROM PAID_ORGANIC_QUERY_REPORT
                WHERE CampaignStatus=ENABLED AND AdGroupStatus=ENABLED AND OrganicClicks>0 AND Clicks >1 AND
                      OrganicAveragePosition < {}
                DURING 20160909,20161009
                """.format(10)

    df2 = get_report_for_accounts(query, [el[1] for el in all_accounts], n_jobs=10)

    res = pd.merge(df1, df2, how='inner', on=['ad_group_id', 'campaign_id', 'client_id', 'keyword_id'])
    # query = """
    #     SELECT  AccountDescriptiveName, AdGroupCriteriaCount, AdGroupsCount, LabelId, LabelName
    #     FROM LABEL_REPORT
    #     """
    #
    # df_labels = get_report_for_accounts(query, [el[1] for el in all_accounts], n_jobs=10)

    to_pause = res[(res['organic_average_position'] > 1.5)
                   & (res['conversions'] < 10)
                   & (res['conversions'] / res['organic_clicks'] < 0.1)]

    l_non_existent_currency = ['CLP', 'ARS', 'COP', 'AED']
    filtered1 = to_pause[~to_pause.currency.isin(l_non_existent_currency)]
    filtered2 = to_pause[to_pause.currency.isin(l_non_existent_currency)]

    to_pause1 = add_fx_rate_column(filtered1, currency_code_col='currency')
    to_pause2 = filtered2.copy()
    to_pause2.loc[to_pause2[to_pause2['currency'] == 'CLP'].index, 'fx_rate'] = 0.001410
    to_pause2.loc[to_pause2[to_pause2['currency'] == 'ARS'].index, 'fx_rate'] = 0.060440
    to_pause2.loc[to_pause2[to_pause2['currency'] == 'COP'].index, 'fx_rate'] = 0.000310
    to_pause2.loc[to_pause2[to_pause2['currency'] == 'AED'].index, 'fx_rate'] = 0.24967

    to_pause = pd.concat([to_pause1, to_pause2])

    # res1 = add_fx_rate_column(res, currency_code_col='currency')
    to_pause['cost_euro'] = to_pause['cost'] * to_pause['fx_rate']

    return to_pause


def _ad_pauser_pause(client, to_pause, validate_only=True):

    kw_operations = get_update_operations(to_pause)

    to_pause = to_pause.to_dict('records')

    # Create label called 'Paused' if it doesn't exist
    label_pause = 'Paused SEO toolbox'

    client.validate_only = False
    initialize_label_placeholders(None, [label_pause], client)

    client.validate_only = validate_only
    added, failed = batch_add_labels_to_keywords(client, [(el['ad_group_id'], el['keyword_id']) for el in to_pause],
                                                 label_names=[label_pause]
                                                 )

    ad_group_criterion_service = client.GetService('AdGroupCriterionService', version='v201607')

    added, failed = partial_failure_process_chunks(ad_group_criterion_service, kw_operations)

    return added, failed


def ads_pauser(client_id='560-035-7116'):
    client = get_default_production_client(client_id)
    to_pause = _ad_pauser_decide_keywords_to_pause(client)

    # duplicates = to_pause[to_pause.duplicated(['keyword_id', 'ad_group_id', 'client_id'], keep=False)]
    # to_pause = to_pause.drop_duplicates(['keyword_id', 'ad_group_id', 'client_id'])
    t = to_pause.groupby(['keyword_id', 'ad_group_id', 'client_id', 'campaign_id']).mean()
    t.reset_index(inplace=True)

    return _ad_pauser_pause(client, t)


# To do:
# data in last 1 month/ 3/6 months (latter two divided by corresponding factor (3/6)) - done
# Plot side by side - done
# Theta range from 0.005 to 0.05 - done
# CTR threshold try with 0.005 and compare with old result - done
def threshold_savings_impact_plotter(df):
    thetas = np.arange(0.005, 1, 0.01)
    savings = np.zeros(len(thetas))

    for i in range(len(thetas)):
        df['to_prune'] = (posterior_sf(0.01, df['ad_clicks'], df['conversions'], 0.5, 0.5) <= thetas[i])
        savings[i] = sum(df['to_prune']*df['cost_euro'])

    plt.plot(thetas, savings, 'r--')
    # plt.show()
    save("theta_savings_impact_in_euro_GFG_(whole)_0.01", ext="pdf", close=True, verbose=True)


def threshold_savings_impact_comparison(df):
    thetas = np.arange(0.005, 0.5, 0.005)

    savings_exp1 = np.zeros(len(thetas))
    for i in range(len(thetas)):
        df['to_prune'] = (posterior_sf(0.01, df['ad_clicks'], df['conversions'], 0.5, 0.5) <= thetas[i])
        savings_exp1[i] = sum(df['to_prune'] * df['cost_euro'])

    savings_exp2 = np.zeros(len(thetas))
    for i in range(len(thetas)):
        df['to_prune'] = (posterior_sf(0.005, df['ad_clicks'], df['conversions'], 0.5, 0.5) <= thetas[i])
        savings_exp2[i] = sum(df['to_prune'] * df['cost_euro'])

    plt.plot(thetas, savings_exp1)
    plt.plot(thetas, savings_exp2, color='g')
    plt.xlabel(r'$x$ (confidence threshold)')
    plt.ylabel(r'$S(x)$ (money saving in euro)')

    plt.legend([r'$\theta=0.01$', r'$\theta=0.005$'], loc='upper left')

    save("theta_savings_impact_in_euro_GFG_(whole)_theta_comparison", ext="pdf", close=True, verbose=True)





    # Handle if stuff did not work at this stage.

if __name__ == '__main__':
    ads_pauser(5274888718)

    # label_service = client.GetService('LabelService', version='v201607')
    #
    # if not label_result:
    #     label_operation = [
    #         {
    #             'operator': 'ADD',
    #             'operand':
    #             {
    #                 'xsi_type': 'TextLabel',
    #                 'name': label_pause,
    #                 'status': 'ENABLED',
    #             }
    #         }
    #     ]
    #     label_service.mutate(label_operation)
    #
    #     label_result = retrieve_labels(client, label_names=[label_pause])
    #     # batch_add_labels(client, [label_pause,])
    #
    # ad_group_criterion_service = client.GetService('AdGroupCriterionService', version='v201607')
    # ads = ad_group_criterion_service.mutate(kw_operations)
    #
    # added, failed = partial_failure_process_chunks(ad_group_criterion_service, kw_operations)
    #
    # for el in failed:
    #     print(el)
    #
    # # Add label 'Paused' to the adgroup & keyword pairs in the list to_pause
    # pause_label_id = label_result[0]['id']
    # label_link_operations = get_label_operations(to_pause, pause_label_id)
    # ad_group_criterion_service.mutateLabel(label_link_operations)
