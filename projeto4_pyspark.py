# Spark SQL

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import date

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv('clientes-v3-preparado.csv') # criando DataFrame    

df.take(5) # retorna os dados e o formato  

df.show() # retorna tabela 

df.printSchema() # retorna toda a strutura do arquivo 

df = spark.read.option('header','true').cvs('clientes-v3-preparado.csv') # inclui o cabeçalho 

df = spark.read.option('header','true').options('infer.Schema','true').csv('clientes-v3-preparado.csv') # infere os tipos de dados 

df = spark.read.\
    option('sep', ',').\
    option('header', 'true').\
    option('mode', 'DROPMALFORMED').\
    option('inferSchema', 'true').\
    cvs('clientes-v3-preparado.csv')

lista_campos = [ # especifica os mais diferentes tipos de dados 
    StructField('nome', StringType()),
    StructField('cpf', StringType()),
    StructField('idade', FloatType()),
    StructField('data', DateType()),
    StructField('endereço', StringType()),
    StructField('bairro', StringType()),
    StructField('estado', StringType())
]
schema_definido = StructType(lista_campos)

df = spark.read.options('header', 'true').schema(schema_definido).cvs('clientes-v3-preparado.csv')

spark.stop()