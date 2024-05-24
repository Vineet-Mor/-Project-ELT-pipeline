from google.cloud import bigquery
from datetime import datetime
client = bigquery.Client(project="quixotic-treat-419302")

# Getting date, month and year ----------------------------------
current_time=datetime.now().date()
date=current_time.strftime("%y-%m-%d").split("-")[-1]
month=current_time.strftime("%y-%m-%d").split("-")[-2]
year=current_time.strftime("%y-%m-%d").split("-")[-3]
dataset_id=f"quixotic-treat-419302.sales_data_{year}"
#dataset_id=f"sales_data_{year}"
# Creating BigQuery dataset -------------------------------------
try:
    client.get_dataset(dataset_id)  # Make an API request.
    print("Dataset {} already exists".format(dataset_id))
except:
    dataset = bigquery.Dataset(dataset_id)
    print(dataset)
    dataset = client.create_dataset(dataset, timeout=30) 
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

# Defining schema for sales, customer, product tables ------------------------------
sales_schema=[
    bigquery.SchemaField("SaleId", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("OrderId", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("ProductId", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("Quantity", "INTEGER", mode="REQUIRED"),
    ]
customer_schema = [
    bigquery.SchemaField("CustomerId", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("Active", "BOOLEAN", mode="REQUIRED"),
    bigquery.SchemaField("Name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Address", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("City", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Country", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Email", "STRING", mode="REQUIRED"),
    ]
product_schema=[
    bigquery.SchemaField("ProductId", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("Name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ManufacturedCountry", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("WeightGrams", "INTEGER", mode="REQUIRED"),
    ]

# Defining table id for each tables i.e. sales, product, customer -------------------------
dataset_id=f"sales_data_{year}"
sales_table_id = dataset_id+f".sales_{year}"
customer_table_id = dataset_id+f".customer_{year}"
product_table_id = dataset_id+f".product_{year}"

# Defining function to create bigquery tables ---------------------------------------------
def create_bigquery_table(table_id,schema):
    try:
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)  # Make an API request.
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )
    except:
        print(f"{table_id} already exist")

#Creating BigQuery tables --------------------------------------------------------------------
create_bigquery_table(sales_table_id,sales_schema)
create_bigquery_table(product_table_id,product_schema)
create_bigquery_table(customer_table_id,customer_schema)

# Defining source for each tables in uri format ---------------------------------------------
project_id = "quixotic-treat-419302"
#sales_uri =f"gs://new_project_data_vm/sales_data/sales_{date}_{month}_{year}.csv"
#customer_uri=f"gs://new_project_data_vm/sales_data/customers_{date}_{month}_{year}.csv"
#product_uri=f"gs://new_project_data_vm/sales_data/products_{date}_{month}_{year}.csv"
sales_uri ="gs://new_project_data_vm/sales_data/sales.csv"
customer_uri="gs://new_project_data_vm/sales_data/customers.csv"
product_uri="gs://new_project_data_vm/sales_data/products.csv"

# Defining function to create external tables for each table ---------------------------------- 
def create_external_table(client, dataset_id, table_id, source_uris, schema, file_format='CSV'):
    table_ref = client.dataset(dataset_id).table(table_id)
    
    external_config = bigquery.ExternalConfig(file_format)
    external_config.source_uris = source_uris
    external_config.schema = schema
    if file_format == 'CSV':
        external_config.options.skip_leading_rows = 1  
    table = bigquery.Table(table_ref)
    table.external_data_configuration = external_config

    table = client.create_table(table, exists_ok=True)
    print(f"Created external table {table.full_table_id}")


# Create external tables -------------------------------------------------------------------------
dataset_id=f"sales_data_{year}"
create_external_table(client, dataset_id, f'external_sales_{year}', [sales_uri], sales_schema)
create_external_table(client, dataset_id, f'external_product_{year}', [product_uri], product_schema)
create_external_table(client, dataset_id, f'external_customer_{year}', [customer_uri], customer_schema)

# Defining function to execute queries -------------------------------------------------------------
def execute_query(client, query):
    job = client.query(query)
    job.result()  # Wait for the job to complete
    print(f"Executed query: {query}")

# Defining external table id for each tables i.e. sales, product, customer -------------------------
dataset_id=f"sales_data_{year}"
external_sales_table_id = dataset_id+f".external_sales_{year}"
external_customer_table_id = dataset_id+f".external_customer_{year}"
external_product_table_id = dataset_id+f".external_product_{year}"

# Defining upsert queries for sales, product, customer tables
# Upsert sales data query 
upsert_sales_query = f"""
MERGE `{sales_table_id}` T
USING `{external_sales_table_id}` S
ON   T.SaleId=S.SaleId AND T.OrderId=S.OrderId AND T.ProductId=S.ProductId AND T.Quantity=S.Quantity
WHEN MATCHED THEN
  UPDATE SET T.SaleId=S.SaleId, T.OrderId=S.OrderId, T.ProductId=S.ProductId, T.Quantity=S.Quantity
WHEN NOT MATCHED THEN
  INSERT (SaleId, OrderId, ProductId, Quantity)
  VALUES (S.SaleId, S.OrderId, S.ProductId, S.Quantity)
"""

# Upsert products data query 
upsert_products_query =f"""
MERGE `{product_table_id}` T
USING `{external_product_table_id}` S
ON   T.ProductId=S.ProductId AND T.Name=S.Name AND T.ManufacturedCountry=S.ManufacturedCountry AND T.WeightGrams=S.WeightGrams
    
WHEN MATCHED THEN
  UPDATE SET T.ProductId=S.ProductId, T.Name=S.Name, T.ManufacturedCountry=S.ManufacturedCountry, T.WeightGrams=S.WeightGrams
WHEN NOT MATCHED THEN
  INSERT (ProductId, Name, ManufacturedCountry, WeightGrams)
  VALUES (S.ProductId, S.Name, S.ManufacturedCountry, S.WeightGrams)
"""

# Upsert customers data query
upsert_customers_query = f"""
MERGE `{customer_table_id}` T
USING `{external_customer_table_id}` S
ON   T.CustomerId=S.CustomerId AND T.Active=S.Active AND T.Name=S.Name AND T.Address=S.Address AND T.City=S.City AND T.Country=S.Country AND T.Email=S.Email
WHEN MATCHED THEN
  UPDATE SET T.CustomerId=S.CustomerId, T.Active=S.Active, T.Name=S.Name, T.Address=S.Address, T.City=S.City, T.Country=S.Country, T.Email=S.Email
WHEN NOT MATCHED THEN
  INSERT (CustomerId, Active, Name, Address, City, Country, Email)
  VALUES (S.CustomerId, S.Active, S.Name, S.Address, S.City, S.Country, S.Email)
"""

# Execute upsert queries ----------------------------------------------------------------------
execute_query(client, upsert_sales_query)
execute_query(client, upsert_products_query)
execute_query(client, upsert_customers_query)
