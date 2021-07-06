import mysql.connector as mysql
import pandas as pd

# enter your server IP address/domain name
HOST = "levitationpgh.com" # or "domain.com"
# database name, if you want just to connect to MySQL server, leave it empty
DATABASE = "dhaiyixx_pres363"
# this is the user you create
USER = "dhaiyixx_remoteAccess"
# user password
PASSWORD = "Playdd232!"

QUERY = "SELECT * FROM psqg_orders o LEFT JOIN psqg_customer c ON o.id_customer = c.id_customer LEFT JOIN psqg_order_detail od ON o.id_order = od.id_order"

# connect to MySQL server
print("Trying to connect...")
db_connection = mysql.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD)
cursor = db_connection.cursor()
print("Connected to:", db_connection.get_server_info())

cursor.execute(QUERY)
field_names = [i[0] for i in cursor.description]

data = pd.DataFrame(cursor.fetchall(), columns=field_names)
data = data.dropna(subset=['id_customer'])
data["firstname"] = data["firstname"].fillna("")
data["lastname"] = data["lastname"].fillna("")
data["customer_name"] = data[["firstname", "lastname"]].agg(" ".join, axis=1)

filtered = data[["product_name", "product_quantity", "delivery_date", "id_customer", "customer_name"]]
filtered = filtered.loc[:, ~filtered.columns.duplicated()]
print(filtered.groupby("product_name").sum())
filtered["id_customer"] = filtered["id_customer"].values

print("="*20 + "\n")
customers = filtered.reset_index().groupby("id_customer")
for k,v in customers:
    items = customers.get_group(k)
    print(items["customer_name"].values[0])
    print(items.groupby(["product_name"]).sum()["product_quantity"], "\n\n")