# Bootcamp_Datos

Dentro de Databricks, primero escribí un script para poder visualziar de mejor manera los datos creando un Dataframe:

df = spark.read.format("csv").option("header", True).option("delimiter", ";").load("dbfs:/FileStore/tables/Supertienda.csv") display(df)

Después de crear el Dataframe usé un comando para mostrar el esquema del Dataframe para poder verificar los nombres de las columnas:

df.printSchema()

Después seleccioné las columnas de la lista y apliqué un filtro para seleccionar solo las filas donde el valor de la columna 'Discount' es mayor a 0.1:

df_transformed = df.select("Row ID", "Order ID", "Order Date", "Ship Date", "Ship Mode", "Customer ID", "Customer Name", "Segment", "Country", "City", "State", "Postal Code", "Region", "Product ID", "Category", "Sub-Category", "Product Name", "Sales", "Quantity", "Discount", "Profit").filter(df["Discount"] > 0.1)

Después utilicé un comando para mostrar los primeros registros del Dataframe transformado:

df_transformed.show()

Luego decidí realizar un gráfico de dispersión para averiguar la intensidad de la relación entre las variables 'Sales' y 'Profit':

Para lograr esto primero importé la biblioteca necesaria:

import matplotlib.pyplot as plt

Después usé un comando para mostrar el gráfico en la notebook de Databricks

%matplotlib inline

Después cree un Dataframe con las columnas 'Sales' y 'Profit':

df_sales_profit = df.select("Sales", "Profit").filter(df["Discount"] > 0.1).toPandas()

Luego grafiqué el gráfico de dispersión:

plt.figure(figsize=(10, 6)) plt.scatter(df_sales_profit['Sales'], df_sales_profit['Profit'], alpha=0.5) plt.title('Relación entre Ventas y Ganancias') plt.xlabel('Ventas') plt.ylabel('Ganancias') plt.show()


Una vez que realicé el modelo lógico decidí comenzar a crear las tablas para el modelo físico de la siguiente manera:

Para la entidad Order:

df_order = df.select(
    df["Row ID"].cast("int").alias("Row_ID"),
    df["Order ID"].cast("string").alias("Order_ID"),
    df["Order Date"].cast("timestamp").alias("Order_Date"),
    df["Ship Date"].cast("timestamp").alias("Ship_Date"),
    df["Ship Mode"].cast("string").alias("Ship_Mode"),
    df["Customer ID"].cast("string").alias("Customer_ID")
)

display(df_order)

df_order.write.format("parquet").mode("overwrite").saveAsTable("order_table")

Para la entidad Product:

df_product = df.select(
    df["Product ID"].cast("string").alias("Product_ID"),
    df["Category"].cast("string").alias("Category"),
    df["Sub-Category"].cast("string").alias("Sub-Category"),
    df["Product Name"].cast("string").alias("Product_Name")
)

display(df_product)

df_product.write.format("parquet").mode("overwrite").saveAsTable("product_table")

Para la entidad Order Product:

df_order_product = df.select(
    df["Quantity"].cast("int").alias("Quantity"),
    df["Discount"].cast("decimal(10,2)").alias("Discount"),
    df["Sales"].cast("decimal(10,2)").alias("Sales"),
    df["Profit"].cast("decimal(10,2)").alias("Profit")
)

display(df_order_product)

df_order_product.write.format("parquet").mode("overwrite").saveAsTable("order_product_table")

Para la entidad Ship Mode:

df_ship_mode = df.select(
    df["Ship Mode"].cast("string").alias("Ship_Mode_ID")
)

display(df_ship_mode)

df_ship_mode.write.format("parquet").mode("overwrite").saveAsTable("ship_mode_table")

Para la entidad Customer:

df_customer = df.select(
    df["Customer ID"].cast("string").alias("Customer_ID"),
    df["Customer Name"].cast("string").alias("Customer_Name"),
    df["Segment"].cast("string").alias("Segment"),
    df["Country"].cast("string").alias("Country"),
    df["City"].cast("string").alias("City"),
    df["State"].cast("string").alias("State"),
    df["Postal Code"].cast("string").alias("Postal_Code"),
    df["Region"].cast("string").alias("Region")
)

display(df_customer)

df_customer.write.format("parquet").mode("overwrite").saveAsTable("customer_table")

Todos fueron chequeados posteriormente con SQL usando el comando:

SELECT * FROM nombre_tabla

Y devolvieron todos correctamente los datos.
