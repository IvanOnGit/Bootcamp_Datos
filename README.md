# Bootcamp_Datos

Dentro de Databricks, primero escribí un script para poder visualziar de mejor manera los datos creando un Dataframe:

df = spark.read.format("csv").option("header", True).option("delimiter", ";").load("dbfs:/FileStore/tables/Supertienda.csv")
display(df)

Después de crear el Dataframe usé un comando para mostrar el esquema del Dataframe para poder verificar los nombres de las columnas:

df.printSchema()

Después seleccioné las columnas de la lista y apliqué un filtro para seleccionar solo las filas donde el valor de la columna 'Discount' es mayor a 0.1:

df_transformed = df.select("`Row ID`", "`Order ID`", "`Order Date`", "`Ship Date`", "`Ship Mode`",
                           "`Customer ID`", "`Customer Name`", "`Segment`", "`Country`", "`City`",
                           "`State`", "`Postal Code`", "`Region`", "`Product ID`", "`Category`",
                           "`Sub-Category`", "`Product Name`", "`Sales`", "`Quantity`", "`Discount`",
                           "`Profit`").filter(df["Discount"] > 0.1)

Después utilicé un comando para mostrar los primeros registros del Dataframe transformado:

df_transformed.show()

Luego decidí realizar un gráfico de dispersión para averiguar la intensidad de la relación entre las variables 'Sales' y 'Profit':

Para lograr esto primero importé la biblioteca necesaria: 

import matplotlib.pyplot as plt

Después usé un comando para mostrar el gráfico en la notebook de Databricks

%matplotlib inline

Después cree un Dataframe con las columnas 'Sales' y 'Profit':

df_sales_profit = df.select("`Sales`", "`Profit`").filter(df["Discount"] > 0.1).toPandas()

Luego grafiqué el gráfico de dispersión:

plt.figure(figsize=(10, 6))
plt.scatter(df_sales_profit['Sales'], df_sales_profit['Profit'], alpha=0.5)
plt.title('Relación entre Ventas y Ganancias')
plt.xlabel('Ventas')
plt.ylabel('Ganancias')
plt.show()





      
