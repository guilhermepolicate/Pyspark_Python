# Integrando Dados com PySpark (agrupar / concatenar)

import pyspark 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit

spark = SparkSession.builder.getOrCreate()

produtos = spark.read.csv('produtos.csv', header = True, inferSchema = True)
vendedores = spark.read.csv('vendedores.csv', header = True, inferSchema = True)
clientes = spark.read.csv('clientes.csv', header = True, inferSchema = True)
itens_pedido = spark.read.csv('itens_pedido.csv', header = True, inferSchema = True)
pagamentos_pedido = spark.read.csv('pagamentos_pedido.csv', header = True, inferSchema = True)
avaliacoes_pedido = spark.read.csv('avaliacoes_pedido.csv', header = True, inferSchema = True)
pedidos = spark.read.csv('padidos', header = True, inferSchema = True)

# Join em Spark
join_pedidos_clientes_df = pedidos.join(clientes, pedidos['id_cliente'] == clientes['id_cliente'], 'inner')
pedidos.show(5)
clientes.show(5)
join_pedidos_clientes_df.show(5)

# Join com filtro para clientes com pedidos cancelados 
join_pedidos_clientes_df = pedidos.filter(col('status_pedido') == 'canceled').join(clientes, 'id_clientes')

# Join crianco campo com concat
pedidos_concat_df = pedidos.withColumn('id_pedido_cliente', concat(col('id_pedido'), lit('-'), col('id_pedido')))

# Colocando a nova coluna no começo do DF 
colunas = pedidos_concat_df.columns
print(colunas)

colunas.remove('id_pedido_cliente')
colunas.insert(0, 'id_pedido_cliente')

pedidos_reorganizar_df = pedidos_concat_df.select(colunas)
pedidos_reorganizar_df.show(n=5, truncate = False)

# Salvando arquivo no formato parquet
join_pedidos_clientes_df.write.mode('overwrite').option('header', 'true').parquet('join_itens_pedido_parquet')
spark.read.option('header', 'true').parquet('join_itens_pedido_parquet').printSchema()
spark.stop()