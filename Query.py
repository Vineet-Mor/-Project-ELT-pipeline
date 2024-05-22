#EXECUTING QUERY TO JOIN SALES TABLE AND PRODUCT TABLE TO FIND THE PRODUCTS SOLD WHOSE WEIGHT IS LESS THAN 100 GRAMS:  
Query1="SELECT a.ProductId, a.Name, a.WeightGrams, a.ManufacturedCountry, b.SaleId, b.Quantity FROM sales_05_2024.product_2024_05_21 a JOIN sales_05_2024.sales_2024_05_21 b ON (a.ProductId=b.ProductId) WHERE a.WeightGrams<100 ORDER BY 1"

query1_job=client.query(Query1)
rows1=query1_job.result()
for row in rows1:
    print(row)
