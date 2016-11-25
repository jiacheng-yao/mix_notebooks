import psycopg2
import pandas as pd
import StringIO

try:
    conn = psycopg2.connect("host='localhost' port='5432' dbname='postgres' user='yao.jiacheng' password=''")
except:
    print "Unable to connect to the database"

table_name = 'dwh_il.fct_orders'

cur = conn.cursor()

df = pd.read_csv('/Users/yao.jiacheng/Documents/mix notebooks/{}.csv'.format(table_name), encoding='utf8')

df = df.replace({',': ' ', '\r':'', '\n':' '}, regex=True)

id_columns = [col for col in df.columns if '_id' in col]

if table_name == 'dwh_il.fct_orders':
    id_columns.append('preorder')
    id_columns.append('expected_delivery_min')
    id_columns.append('edited')
    id_columns.append('order_hour')
    id_columns.append('online_payment')
    id_columns.append('first_order_all')
    id_columns.append('first_order')
    id_columns.append('first_app_order')
    id_columns.append('promised_delivery_time')

for col in id_columns:
    df[col] = df[col].fillna(0)
    df[col] = df[col].astype(int)

output = StringIO.StringIO()
df.to_csv(output, sep=',', header=False, index=False, encoding='utf8')
output.seek(0)

SQL_STATEMENT = """
    COPY {} FROM STDIN WITH
        CSV
        DELIMITER AS ','
    """
cur.copy_expert(sql=SQL_STATEMENT.format(table_name), file=output)
conn.commit()
cur.close()
