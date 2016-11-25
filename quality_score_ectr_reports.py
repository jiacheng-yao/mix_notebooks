import os

os.environ['ROCKETADS_CONFIG_FILE'] = '/Users/yao.jiacheng/Documents/rocketads/settings.yml'
os.environ['QUANDL_AUTH_TOKEN'] = 'ReYeREgDws3cP8qX_V1P'
import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import seaborn as sns

from StringIO import StringIO
from googleads import AdWordsClient, oauth2

from rocketads.adwords.hierarchy import get_mcc_hierarchy_raw
from rocketads.adwords.reports import get_report_for_accounts
from rocketads.oauth2.credentials import get_default_production_client, create_adwords_client, TOKEN
from rocketads.adwords.account_operations import batch_add_labels, retrieve_labels
from rocketads.adwords.account_operations import partial_failure_process_chunks, partial_failure_mutate_label_chunks, \
    batch_add_labels_placeholders, batch_add_labels_to_keywords, initialize_label_placeholders
from rocketads.utils.currency_conversion import add_fx_rate_column

from plt_save import save


def find_venture(start, root, graph):
    check = set(el.customerId for el in graph.entries)
    assert start in check and root in check

    def get_el(customer_id):
        for el in graph.entries:
            if el.customerId == customer_id:
                return el.name, el.customerId

    current = start
    while True:
        for el in graph.links:
            if el.managerCustomerId == root and el.clientCustomerId == current:
                return get_el(el.clientCustomerId)

            elif el.clientCustomerId == current:
                current = el.managerCustomerId


def df_retriever():
    client = get_default_production_client('560-035-7116')

    graph_all = get_mcc_hierarchy_raw(client, True)
    # all_accounts = [customer.customerId for customer in graph_all.entries]
    all_accounts_non_mcc = [customer.customerId for customer in graph_all.entries if customer.canManageClients == False]

    query = """
        SELECT  ExternalCustomerId, AccountDescriptiveName, QualityScore, SearchPredictedCtr, PostClickQualityScore, CreativeQualityScore
        FROM KEYWORDS_PERFORMANCE_REPORT
        WHERE AdGroupStatus=ENABLED AND CampaignStatus=ENABLED AND Status=ENABLED
        AND HasQualityScore=TRUE
        """

    df1 = get_report_for_accounts(query, all_accounts_non_mcc, n_jobs=100)
    df1.drop('client_id', axis=1, inplace=True)

    def key_value_gen(k):
        yield k
        yield find_venture(k, 5600357116, graph_all)[0]

    def key_value_gen_id(k):
        yield k
        yield find_venture(k, 5600357116, graph_all)[1]

    d = dict(map(key_value_gen, df1.customer_id.unique()))
    d_id = dict(map(key_value_gen_id, df1.customer_id.unique()))

    df1['venture_id'] = df1.apply(lambda row: d_id[row['customer_id']], axis=1)
    df1['venture_name'] = df1.apply(lambda row: d[row['customer_id']], axis=1)

    df_pivoted = pd.pivot_table(df1, index=['venture_name', 'expected_clickthrough_rate'],
                                values=['quality_score'], aggfunc=('count', 'mean'))

    return df1


def df_retriever_dh():
    def _get_refresh_token(use_delivery_hero):

        if use_delivery_hero:
            refresh_token = os.getenv('REFRESH_TOKEN_DELIVERY_HERO', None)
        else:
            refresh_token = os.getenv('REFRESH_TOKEN', None)

        if not refresh_token:
            raise ValueError('Refresh Token can not be empty, please set environment variables')

        return refresh_token

    def get_adwords_client(account_id, use_delivery_hero=False):

        token_client = oauth2.GoogleRefreshTokenClient(
            client_id=os.getenv('CLIENT_ID'),
            client_secret=os.getenv('CLIENT_SECRET'),
            refresh_token=_get_refresh_token(use_delivery_hero)
        )

        adwords_client = AdWordsClient(
            developer_token=os.getenv('ADWORDS_DEVELOPER_TOKEN'),
            oauth2_client=token_client,
            user_agent='rocketads-script'
        )

        adwords_client.SetClientCustomerId(account_id)

        return adwords_client

    os.environ['CLIENT_ID'] = '802948045536-78peaitnuteptdc9grs1dk68966n2sk1.apps.googleusercontent.com'
    os.environ['CLIENT_SECRET'] = 'qiUOT5lKqgGxielU3mkqxWAt'
    os.environ['REFRESH_TOKEN'] = '1/o65h1yWfFvJm8jjxsMXpI6huBM1QnSf-UydHJ_TTYZs'
    os.environ['REFRESH_TOKEN_DELIVERY_HERO'] = '1/o65h1yWfFvJm8jjxsMXpI6huBM1QnSf-UydHJ_TTYZs'
    os.environ['ADWORDS_DEVELOPER_TOKEN'] = 'sYfaQrAC9jfjPOTYcX9sgQ'

    client_id = 1771489218
    use_delivery_hero = True
    include_mccs = True
    API_VERSION = 'v201607'

    client = get_adwords_client(client_id, use_delivery_hero)

    managed_customer_service = client.GetService('ManagedCustomerService',
                                                 version=API_VERSION)
    # Construct selector to get all accounts.
    selector = {
        'fields': ['CustomerId', 'Name', 'CanManageClients',
                   'CurrencyCode', 'DateTimeZone']
    }
    # Get serviced account graph.
    graph_all = managed_customer_service.get(selector)
    if not include_mccs:
        # import ipdb; ipdb.set_trace()
        graph_all.entries = [el for el in graph_all.entries if not el.canManageClients]

    # all_accounts = [customer.customerId for customer in graph_all.entries]
    all_accounts_non_mcc = [customer.customerId for customer in graph_all.entries if customer.canManageClients == False]

    reports = list()
    tmp_idx = 0

    for client_id in all_accounts_non_mcc:
        query = """
            SELECT  ExternalCustomerId, AccountDescriptiveName, QualityScore, SearchPredictedCtr, PostClickQualityScore, CreativeQualityScore
            FROM KEYWORDS_PERFORMANCE_REPORT
            WHERE AdGroupStatus=ENABLED AND CampaignStatus=ENABLED AND Status=ENABLED
            AND HasQualityScore=TRUE
            """
        client.client_customer_id = client_id
        report_downloader = client.GetReportDownloader(version='v201607')

        if tmp_idx % 50 == 1:
            print 'downloading report No.{}...'.format(tmp_idx)
        tmp_idx = tmp_idx + 1

        data = StringIO(report_downloader.DownloadReportAsStringWithAwql(
            query,
            'CSV',
            skip_report_header=True,
            skip_report_summary=True,
            skip_column_header=False
        ))

        report = pd.read_csv(data, encoding='utf-8', sep=',')
        reports.append(report)
    df1 = pd.concat(reports)
    df1.columns = [p.lower().replace(' ', '_').strip() for p in df1.columns]
    df1.drop('client_id', axis=1, inplace=True)

    def key_value_gen(k):
        yield k
        yield find_venture(k, 1771489218, graph_all)[0]

    def key_value_gen_id(k):
        yield k
        yield find_venture(k, 1771489218, graph_all)[1]

    d = dict(map(key_value_gen, df1.customer_id.unique()))
    d_id = dict(map(key_value_gen_id, df1.customer_id.unique()))

    df1['venture_id'] = df1.apply(lambda row: d_id[row['customer_id']], axis=1)
    df1['venture_name'] = df1.apply(lambda row: d[row['customer_id']], axis=1)

    df_pivoted = pd.pivot_table(df1, index=['venture_name', 'expected_clickthrough_rate'],
                                values=['quality_score'], aggfunc=('count', 'mean'))

    return df1


def bar_chart_plotter(df, mcc='Rocket'):
    df_pivoted_mean = pd.pivot_table(df, index=['venture_name'],
                                  values=['quality_score'], aggfunc='mean')
    df_pivoted_std = pd.pivot_table(df, index=['venture_name'],
                                  values=['quality_score'], aggfunc='std')

    df_qs_mean = df_pivoted_mean['quality_score']
    df_qs_std = df_pivoted_std['quality_score']

    N = len(df_qs_mean)  # number of data entries
    ind = np.arange(N)  # the x locations for the groups
    width = 0.35

    fig, ax = plt.subplots()

    rects1 = ax.barh(ind, df_qs_mean,  # data
                    width,  # bar width
                    color='MediumSlateBlue',  # bar colour
                    xerr=df_qs_std,  # data for error bars
                    error_kw={'ecolor': 'Tomato',  # error-bars colour
                              'linewidth': 2})  # error-bar width

    axes = plt.gca()
    axes.set_xlim([0, 11])

    ax.set_xlabel('Quality Score')
    ax.set_title('Quality Score by venture ({})'.format(mcc))
    ax.set_yticks(ind + width)
    ax.set_yticklabels(df_pivoted_mean.index)
    ax.tick_params(axis='y', which='major', pad=15)

    def autolabel(rects, ax):
        # Get y-axis height to calculate label position from.
        (y_bottom, y_top) = ax.get_ylim()
        y_height = y_top - y_bottom

        for rect in rects:
            height = rect.get_height()

            # Fraction of axis height taken up by this rectangle
            p_height = (height / y_height)

            # If we can fit the label above the column, do that;
            # otherwise, put it inside the column.
            if p_height > 0.95: # arbitrary; 95% looked good to me.
                label_position = height - (y_height * 0.05)
            else:
                label_position = height + (y_height * 0.01)

            ax.text(rect.get_x() + rect.get_width()/2., label_position,
                    '%d' % int(height),
                    ha='center', va='bottom')

    autolabel(rects1, ax)

    save('qs_by_venture_plot({})'.format(mcc), 'pdf')
    plt.show()


def customize_barh(df, mcc='Rocket'):
    df_pivoted_mean = pd.pivot_table(df, index=['venture_name'],
                                     values=['quality_score'], aggfunc='mean')

    df_pivoted_std = pd.pivot_table(df, index=['venture_name'],
                                    values=['quality_score'], aggfunc='std')

    plt.figure(figsize=(20, 20))

    lst_venture = list(df_pivoted_mean.index)
    lst_venture = [l[:30] for l in lst_venture]

    qs_per_venture = pd.DataFrame({
        'qs_mean': list(df_pivoted_mean.quality_score),
        'qs_std': list(df_pivoted_std.quality_score),
    }, index=lst_venture)
    ax = qs_per_venture.qs_mean.plot(kind='barh', xerr=list(df_pivoted_std.quality_score*0.5))

    axes = plt.gca()
    axes.set_xlim([0, 11])

    ax.set_xlabel('Quality Score')
    ax.set_title('Quality Score by venture ({})'.format(mcc))
    ax.get_xaxis().set_major_formatter(plt.FuncFormatter(lambda x, loc: "{:,}".format(int(x))))
    ax.patch.set_facecolor('#FFFFFF')
    ax.spines['bottom'].set_color('#CCCCCC')
    ax.spines['bottom'].set_linewidth(1)
    ax.spines['left'].set_color('#CCCCCC')
    ax.spines['left'].set_linewidth(1)

    ax.yaxis.set_ticklabels(lst_venture, fontstyle='italic')

    save('qs_by_venture_plot({})'.format(mcc), 'pdf')
    plt.show()


def result_generator():
    mcc = 'RKT'
    print 'reading the data...'
    # bar_chart_plotter(df1)

    if mcc=='DH':
        df1 = pd.read_csv('qs_report_dh.csv', encoding='utf-8')
    else:
        df1 = pd.read_csv('qs_report_rocket.csv', encoding='utf-8')

    df1.drop('Unnamed: 0', axis=1, inplace=True)


    print 'writing the pivot table to csv output...'
    df_pivoted = pd.pivot_table(df1, index=['venture_name', 'expected_clickthrough_rate'],
                                values=['quality_score'], aggfunc=('count', 'mean'))
    df_pivoted.to_csv('qs_pivot_report({}).csv'.format(mcc), encoding='utf8')

    print 'plotting the bar chart and saving the results...'
    # bar_chart_plotter(df1)

    # colors_list = ['0.5', 'r', 'b', 'g'] #optional
    customize_barh(df1, mcc)


def result_generator_w_percentage(mcc='merged'):
    df = pd.read_csv('qs_report_full_{}.csv'.format(mcc), encoding='utf-8')
    df.drop('Unnamed: 0', axis=1, inplace=True)

    def perc_calculator(df):
        df_pivoted_1 = pd.pivot_table(df, index=['top_mcc', 'venture_name', 'expected_clickthrough_rate'],
                                      values=['quality_score'], aggfunc=('count', 'mean'))

        df_pivoted_2 = pd.pivot_table(df, index=['top_mcc', 'venture_name', 'landing_page_experience'],
                                      values=['quality_score'], aggfunc=('count', 'mean'))
        df_pivoted_3 = pd.pivot_table(df, index=['top_mcc', 'venture_name', 'ad_relevance'],
                                  values=['quality_score'], aggfunc=('count', 'mean'))

        df_pivoted = pd.concat((df_pivoted_1, df_pivoted_2, df_pivoted_3), axis=1)
        df_pivoted.columns = ['ectr_count', 'ectr_mean', 'lpe_count', 'lpe_mean', 'ar_count', 'ar_mean']

        l_perc_result_1 = []
        l_perc_result_2 = []
        l_perc_result_3 = []

        t1_1 = df_pivoted['ectr_count']
        t1_2 = df_pivoted['lpe_count']
        t1_3 = df_pivoted['ar_count']

        t2 = df.groupby(['top_mcc', 'venture_name'])['quality_score'].count()

        for i in range(df_pivoted.shape[0]):
            tmp_perc = 1.0 * t1_1[i] / t2[i / 3]
            l_perc_result_1.append(tmp_perc)

            tmp_perc = 1.0 * t1_2[i] / t2[i / 3]
            l_perc_result_2.append(tmp_perc)

            tmp_perc = 1.0 * t1_3[i] / t2[i / 3]
            l_perc_result_3.append(tmp_perc)

        df_pivoted['ectr_percentage'] = l_perc_result_1
        df_pivoted['lpe_percentage'] = l_perc_result_2
        df_pivoted['ar_percentage'] = l_perc_result_3

        df_pivoted = df_pivoted[['ectr_count', 'ectr_mean', 'ectr_percentage',
                                 'lpe_count', 'lpe_mean', 'lpe_percentage',
                                 'ar_count', 'ar_mean', 'ar_percentage']]

        return df_pivoted

    df_pivoted_final = perc_calculator(df)

    df_pivoted_final.to_csv('qs_pivot_report_full({}).csv'.format(mcc), encoding='utf8')

if __name__ == "__main__":
    # mcc = 'RKT'
    #
    # df1 = df_retriever()
    # df1.to_csv('qs_report_full_{}.csv'.format(mcc), encoding='utf8')
    result_generator_w_percentage()

# save('test_7', 'pdf')
# plt.show()

# sns.set_style("whitegrid")
# ax = sns.barplot(x="quality_score", y="venture_name", data=df1)
#
# from textwrap import wrap
# labels=['Really really really really really really long label 1',
#         'Really really really really really really long label 2',
#         'Really really really really really really long label 3']
# labels = [ '\n'.join(wrap(l, 20)) for l in labels ]
#
# ax.set_xticklabels(df_pivoted_mean.index)
#
# save('test4_sns', 'pdf')
