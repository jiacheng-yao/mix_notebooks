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
from rocketads.utils.misc import save_intermediate, load_intermediate
# client = get_default_production_client('560-035-7116')
#
# graph_all = get_mcc_hierarchy_raw(client, False)
#
#
# def find_venture(start, root, graph):
#     check = set(el.customerId for el in graph.entries)
#     assert start in check and root in check
#
#     def get_el(customer_id):
#         for el in graph.entries:
#             if el.customerId == customer_id:
#                 return el.name, el.customerId
#
#     current = start
#     while True:
#         for el in graph.links:
#             if el.managerCustomerId == root and el.clientCustomerId == current:
#                 return get_el(el.clientCustomerId)
#
#             elif el.clientCustomerId == current:
#                 current = el.managerCustomerId
#
#
# all_accounts = [customer.customerId for customer in graph_all.entries]
# save_intermediate('tmp.pkl', all_accounts)
all_accounts = load_intermediate('tmp.pkl')[0]

query = """
    SELECT  ExternalCustomerId, AccountDescriptiveName, Conversions,
    Cost, AccountCurrencyCode, AdNetworkType1
    FROM ACCOUNT_PERFORMANCE_REPORT
    DURING 20160425,20161025
    """
# all_accounts= [5290063461,1958136455,6612220812]
df1 = get_report_for_accounts(query, all_accounts, n_jobs=100)
df1.to_csv('data.csv',index=False, encoding='utf8')

