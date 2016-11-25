import psycopg2
import csv

try:
    conn = psycopg2.connect(database="foodora", host="dwh.foodora.com", user="lfiaschi", password="g9!8{SjK47")
except:
    print "Unable to connect to the database"

table_name = 'dwh_il.fct_orders'
# 'dwh_il.dim_countries'
# 'dwh_il.meta_order_status'
# 'dwh_il.dim_customer'
# 'crm_campaigns.margin_targets'
# 'crm_campaigns.rfm_voucher_table'

# 'crm_campaigns.rft_segmentation_final'

cur = conn.cursor()
cur.execute("""SELECT * from {}
            WHERE order_date >= to_timestamp('2016-01-01 21:24:00', 'yyyy-mm-dd hh24:mi:ss')
              AND order_date <= to_timestamp('2016-02-01 21:25:33', 'yyyy-mm-dd hh24:mi:ss')
            LIMIT 5""".format(table_name))

with open("{}.csv".format(table_name), "wb") as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow([i[0] for i in cur.description]) # write headers
    csv_writer.writerows(cur)

print "Done Writing: {}".format(table_name)
