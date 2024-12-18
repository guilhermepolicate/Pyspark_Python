# Validando a qualidade dos dados

import pyspark 
import pandas as pd 
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

# Validando a coluna preço 
# criando nova coluna com informação da validade e invalidade
itens_pedido = itens_pedido.withColumn('preco_valido',when(col('preco') > 0, 'valido').otherwise('invalido'))
itens_pedido.select('preco', 'preco_valido').show() # imprimindo novo DF
itens_pedido.filter(col('preco_valido') == 'invalido').show() # imprimindo somente os invalidos 

# Validando o status dos produtos 
status_permitidos = ['created', 'approved', 'delivered', 'shipped', 'canceled', 'invoiced', 'processing']
pedidos=pedidos.withColumns('status_valido',when(col('status_pedido').isin(status_permitidos),'valido').otherwise('invalido'))
pedidos.select('id_pedido', 'status_pedido', 'status_valido').show()
pedidos.filter(col('status_valido') == 'invalido').show()

# Contando todas as Colunas para verificar se ha valores nulos
for coluna in clientes.columns:
    clientes.select(count(when(col(coluna).isNull(),coluna)).alias(coluna)).show()

# verificando integridade da tabela com join 
# left_anti = verifica tudo o que não atende a regra para evitar trazer pedidos errados
pedidos_integridade_df = pedidos.join(clientes, pedidos.id_cliente == clientes.id_cliente, 'left_anti')
pedidos_integridade_df.select('id_pedido', 'id_cliente').show()

# verificação de validade com 'when'
# regex = é bom para quando se deseja verificar padrões
clientes_validacao_df=clientes.withColumn('cep_valido',when(col('cep_cliente').rlike(r'^\d{5}$'),'valido').otherwrise('invalido'))

# verificação com 'if'
if 'email_cliente' in clientes.columns:
    clientes_verifica_email_df=clientes.withColumn('email_valido',when(col('email_cliente').contains('@'),'validado').otherwrise('invalido'))
    clientes_verifica_email_df.select('id_cliente', 'email_cliente', 'email_valido').show()