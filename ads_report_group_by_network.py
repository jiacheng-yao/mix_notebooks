import os

os.environ['ROCKETADS_CONFIG_FILE'] = '/Users/yao.jiacheng/Documents/rocketads/settings.yml'
os.environ['QUANDL_AUTH_TOKEN'] = 'ReYeREgDws3cP8qX_V1P'
import pandas as pd
import numpy as np

from rocketads.adwords.hierarchy import get_mcc_hierarchy_raw
from rocketads.adwords.reports import get_report_for_accounts
from rocketads.oauth2.credentials import get_default_production_client, create_adwords_client, TOKEN
from rocketads.adwords.account_operations import batch_add_labels, retrieve_labels
from rocketads.adwords.account_operations import partial_failure_process_chunks, partial_failure_mutate_label_chunks, \
    batch_add_labels_placeholders, batch_add_labels_to_keywords, initialize_label_placeholders
from rocketads.utils.currency_conversion import add_fx_rate_column

client = get_default_production_client('560-035-7116')

graph_all = get_mcc_hierarchy_raw(client, True)
all_accounts = [customer.customerId for customer in graph_all.entries]


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

query = """
    SELECT  ExternalCustomerId, AccountDescriptiveName, Conversions,
    Cost, AccountCurrencyCode, AdNetworkType1, Month, Impressions, Clicks
    FROM ACCOUNT_PERFORMANCE_REPORT
    DURING 20160401,20160930
    """

df1 = get_report_for_accounts(query, all_accounts, n_jobs=100)
df1['venture_id'] = df1.apply(lambda row: find_venture(row['customer_id'], 5600357116, graph_all)[1], axis=1)
df1['venture_name'] = df1.apply(lambda row: find_venture(row['customer_id'], 5600357116, graph_all)[0], axis=1)

l_non_existent_currency = ['CLP', 'ARS', 'COP', 'AED']
filtered1 = df1[~df1.currency.isin(l_non_existent_currency)]
filtered2 = df1[df1.currency.isin(l_non_existent_currency)]

to_pause1 = add_fx_rate_column(filtered1, currency_code_col='currency')
to_pause2 = filtered2.copy()
to_pause2.loc[to_pause2[to_pause2['currency'] == 'CLP'].index, 'fx_rate'] = 0.001348
to_pause2.loc[to_pause2[to_pause2['currency'] == 'ARS'].index, 'fx_rate'] = 0.058910
to_pause2.loc[to_pause2[to_pause2['currency'] == 'COP'].index, 'fx_rate'] = 0.000305
to_pause2.loc[to_pause2[to_pause2['currency'] == 'AED'].index, 'fx_rate'] = 0.24967

test_df = pd.concat([to_pause1, to_pause2])

# res1 = add_fx_rate_column(res, currency_code_col='currency')
test_df['cost_euro'] = test_df['cost'] * test_df['fx_rate']

test_df.drop('client_id', axis=1, inplace=True)

f = {'cost_euro':['sum'], 'conversions':['sum'], 'impressions':['sum'], 'clicks':['sum']}
group_report = test_df.groupby(['venture_name', 'month', 'network']).agg(f)
group_report.to_csv('clinic_report_01.csv',encoding='utf8')

df_pivoted = pd.pivot_table(test_df, index=['venture_name', 'month', 'network'],
                            values=['conversions', 'impressions', 'clicks', 'cost_euro'], aggfunc='sum')
df_pivoted.head()
df_pivoted.to_csv('clinic_report_02.csv',encoding='utf8')


test_df.venture_name=test_df.apply(lambda row: row['venture_name'][:30], axis=1)
writer = pd.ExcelWriter('clinic_report_2.xlsx')

for venture_name in df_pivoted.index.get_level_values(0).unique():
    temp_df = df_pivoted.xs(venture_name, level=0)
    temp_df.to_excel(writer,venture_name)

writer.save()



