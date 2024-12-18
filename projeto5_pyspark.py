# leitura e escrita de dados no spark 

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate() # lendo os dados 
df = spark.read.options('header', 'true').options('inferSchema', 'true').cvs('clientes-v3-preparado.csv')
df.show()

df.write.csv('output/clientes_csv') # escrevendo dados e salvando no formato csv

df.write.mode('overwrite').options('header', 'true').cvs('clientes-v3-preparado.csv')
df = spark.read.options('header', 'true').options('inferSchema', 'true').cvs('clientes-v3-preparado.csv') # leitura do arquivo criado

# outros formatos 

df.write.options('header', 'true').json('output/clientes_json') # Json
df = spark.read.options('header', 'true').cvs('clientes-v3-preparado.json') # leitura do arquivo criado
df.show()

df.write.options('header', 'true').save('options/clientes_parquet') # .Save

df = spark.read.options('header', 'true').parquet('options/clientes_parquet') # Parquet(colunar)
df.show()

# Salvando dados para ser lido em SQL 
df.write.options('header', 'true').saveAsTable('cliente')

df.creatOrReplaceTempView('temp_cliente') # cria uma tabela temporária 

spark.catalog.listDatabase() # lista banco de dados de diferentes ferramentas  

spark.catalog.setCurrentDatabase('default') # seta outros bancos de dados caso necessário

spark.catalog.listTables() # lista todas as tabelas 

tab_df = spark.sql('select * from clientes') # pode usar os comados de SQL
tab_df.show()

spark.stop()