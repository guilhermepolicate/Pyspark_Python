# Limpeza de Dados com PySpark 

import pyspark 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

produtos = spark.read.csv('produtos.csv', header = True, inferSchema = True)
vendedores = spark.read.csv('vendedores.csv', header = True, inferSchema = True)
clientes = spark.read.csv('clientes.csv', header = True, inferSchema = True)
itens_pedido = spark.read.csv('itens_pedido.csv', header = True, inferSchema = True)
pagamentos_pedido = spark.read.csv('pagamentos_pedido.csv', header = True, inferSchema = True)
avaliacoes_pedido = spark.read.csv('avaliacoes_pedido.csv', header = True, inferSchema = True)
pedidos = spark.read.csv('padidos', header = True, inferSchema = True)

# varredura e visualização parcial de todas as tabelas
print('Produtos: \n', produtos.show(n=5, truncate=False)) # Trucate = mostrar todas as colunas
print('Vendedores: \n', vendedores.show(5))
print('Clientes: \n', clientes.show(5))
print('Itens_pedido: \n', itens_pedido.show(5))
print('Pagamentos_pedido: \n', pagamentos_pedido.show(5))
print('Avaliações_pedido: \n', avaliacoes_pedido.show(5))
print('Pedidos: \n', pedidos.show(5))

# formas de acessar colunas 
clientes.select('id_cliente').show(n=1, truncate = False)
clientes.select(col('id_cliente')).show(1)
clientes.select(clientes['id_cliente']).show(1)
clientes.select(clientes.id_cliente).show(1)

# Tratando valores nulos, criando novos df para evitar perdas

# 1.a - especifica valores nulos do campo "categoria_produto"
produtos_tratar_categ_nulos = produtos.na.fill({'categoria_produto': 'Não Especificado'})

# ou 

# 1.b - filtrando pela categoria os valores nulos a serem tratados 
produtos_tratar_categ_nulos.filter(col('categoria_produto') == 'Não especificado').count() #conta numero de registros alterados

# 2.a - outra forma de tratar valores nulos 
print('totalde pedidos: ', pedidos.count()) # conta o total de pedidos 

pedidos_unicos = pedidos.dropDuplicates() # elimina os registros duplicados
print('total de pedidos únicos: ', pedidos_unicos.count())

pedido_remocao_nulos = pedidos_unicos.na.drop() # limpa os campos nulos 
print('total de pedidos apos a limpeza: ', pedido_remocao_nulos.count())

pedido_remocao_nulos_id = pedidos_unicos.na.drop(subset=['id_cliente', 'id_pedido']) # so remove quando os campos especificados forem nulos 
print('total de pedidos apos limpeza específica: ', pedido_remocao_nulos_id.count())

# 3.a - corrigindo valores nulos de varias colunas ao mesmo tempo 
colunas = ['peso_produto_g', 'comprimento_produto_cm', 'altura_produto_cm', 'largura _produto_cm']
for coluna in colunas:
    produtos = produtos.na.fill({coluna: 0 }) # substitui campo nulo por 0 nas colunas especificadas 

# 4  salvando as alterações
produtos.write.mode('overwrite').option('header', 'true').csv('produtos_tratado.csv')

# 5 lendo o documento alterado salvo 
spark.read.options('header', 'true').csv('produtos_tratado.csv').show(5)