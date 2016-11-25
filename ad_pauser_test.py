import os
os.environ['ROCKETADS_CONFIG_FILE'] = '/Users/yao.jiacheng/Documents/rocketads/settings.yml'
os.environ['TEST_CREDENTIALS_DIR'] = '/Users/yao.jiacheng/Documents/rocketads/'
os.environ['TEST_OAUTH2_CLIENT_ID'] = '796457591545.apps.googleusercontent.com'
from rocketads.adwords.reports import get_report_for_accounts
from rocketads.oauth2.credentials import get_default_development_client
from ads_pauser import _ad_pauser_pause

import pandas as pd


def get_some_test_data_to_pause():
    client = get_default_development_client(5274888718)
    list_clients = [8633739659,9621584361]

    query = """
            SELECT  Id, AdGroupId, CampaignId, Conversions, KeywordMatchType
            FROM KEYWORDS_PERFORMANCE_REPORT
            WHERE CampaignStatus=ENABLED AND AdGroupStatus=ENABLED AND Status=ENABLED
            DURING LAST_MONTH
            """

    df1 = get_report_for_accounts(query, list_clients, get_default_development_client, n_jobs=10)
    query = """
                SELECT  KeywordTextMatchingQuery, KeywordId, AdGroupId, CampaignId, OrganicAveragePosition, OrganicClicks,
                OrganicImpressions, Clicks, SearchQuery
                FROM PAID_ORGANIC_QUERY_REPORT
                WHERE CampaignStatus=ENABLED AND AdGroupStatus=ENABLED
                DURING LAST_MONTH
                """

    df2 = get_report_for_accounts(query, list_clients, get_default_development_client, n_jobs=10)

    res = pd.merge(df1, df2, how='inner', on=['ad_group_id', 'campaign_id', 'client_id', 'keyword_id'])

    to_pause = res[(res['organic_average_position'] > 1.5)
                   & (res['conversions'] < 10)
                   & (res['conversions'] / res['organic_clicks'] < 0.1)]

    return client, to_pause


def test_pausing():

    client, to_pause = get_some_test_data_to_pause()
    _ad_pauser_pause(client, to_pause, True)


if __name__ == '__main__':
    test_pausing()