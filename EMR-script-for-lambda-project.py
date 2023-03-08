
# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime,date

# COMMAND ----------

spark = SparkSession.builder.appName("Lambda-project").getOrCreate()
todays_file_name = str(date.today()) + ".csv"
output_folder_name = str(date.today())

# COMMAND ----------

products = spark.read.csv(f"s3://products/{todays_file_name}",inferSchema=True,header=True)
orders = spark.read.csv(f"s3://lambda-daily-orders/{todays_file_name}",inferSchema=True,header=True)
order_items = spark.read.csv(f"s3://order_items/{todays_file_name}",inferSchema=True,header=True)
departments = spark.read.csv(f"s3://departments/{todays_file_name}",inferSchema=True,header=True)
customers = spark.read.csv(f"s3://customers/{todays_file_name}",inferSchema=True,header=True)
categories = spark.read.csv(f"s3://categories/{todays_file_name}",inferSchema=True,header=True)


# COMMAND ----------

products.cache()
departments.cache()
customers.cache() 
categories.cache()

# COMMAND ----------

#products.printSchema()
#orders.printSchema()
#order_items.printSchema()
#departments.printSchema()
#customers.printSchema()
#categories.printSchema()

# COMMAND ----------

# Customer_orders_infotable of customers whose orders are completed or closed 
#(customer_id,customer_fname+customer_lname,customer_email,order_id,order_date,order_status)
# Customers_orderitems_info to get info about orders that customers placed along with order items info in that order
#(customer_id,customer_fname+customer_lname,customer_email,order_id,order_date,order_status,order_item_id,order_item_quantity,order_item_subtotal)
#Customers_orders_category info to get an idea about which category customers are spending money that we can use to derive actionable insights
#(customer_id,customer_fname+customer_lname,customer_email,order_id,order_item_id,product_id, product_name,product_category_id,category_name)


# COMMAND ----------

# Customer_orders_infotable of customers whose orders are completed or closed 
customers_orders_infotable = customers.join(orders,customers.customer_id == orders.order_customer_id,"inner")\
.select(customers.customer_id,concat(customers.customer_fname,lit(" "),customers.customer_lname).alias("customer_name"),customers.customer_email,orders.order_id,orders.order_date,orders.order_status)

# COMMAND ----------

# Customers_orderitems_info to get info about orders that customers placed along with order items info in that order
customers_orderitems_info = customers.join(orders,customers.customer_id == orders.order_customer_id,"inner").join(order_items,orders.order_id == order_items.order_item_order_id,"inner")\
.select(customers.customer_id,concat(customers.customer_fname,lit(" "),customers.customer_lname).alias("customer_name"),customers.customer_email,orders.order_id,orders.order_date,orders.order_status,order_items.order_item_id,order_items.order_item_quantity,order_items.order_item_subtotal)

# COMMAND ----------

#Customers_orders_category info to get an idea about which category customers are spending money that we can use to derive actionable insights
customers_orders_category = customers.join(orders,customers.customer_id == orders.order_customer_id,"inner").\
    join(order_items,orders.order_id == order_items.order_item_order_id,"inner").\
        join(products,products.product_id == order_items.order_item_product_id,"inner").\
            join(categories,products.product_category_id == categories.category_id,"inner")\
                .select(customers.customer_id,concat(customers.customer_fname,\
                                     lit(" "),customers.customer_lname).alias("customer_name"),customers.customer_email,\
                                            orders.order_id,order_items.order_item_id,\
                                                    products.product_id,products.product_name,categories.category_id,categories.category_name)

# COMMAND ----------
#saving files as each table after processing and transformations (after this we can run a glue job or diretly load these files into any OLAP data warehouse like Redshift and others)
customers_orders_infotable.coalesce(1).write.csv(f"s3://s3-lambda-to-s3-project/output/{output_folder_name}/1/")
customers_orderitems_info.coalesce(1).write.csv(f"s3://s3-lambda-to-s3-project/output/{output_folder_name}/2/")
customers_orders_category.coalesce(1).write.csv(f"s3://s3-lambda-to-s3-project/output/{output_folder_name}/3/")

# COMMAND ----------


