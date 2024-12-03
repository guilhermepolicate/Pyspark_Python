# Trabalhando com RDD

import pyspark
from pyspark import SparkContext

sc = SparkContext.getOrCreate() # Variavel igual ao sparkcontext que ira criar 

rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) # Distribui a coleção localmente com o RDD
rdd.getNumPartitions() # verifica quantas partições 
rdd.first() # retora o primeiro valor
rdd.take(5) # retorna os 5 primeiros valores 
rdd.collect() # cuidado pois retorna toda a coleção de dados que pode ser enorme

rddtexto = sc.parallelize(['Spark', 'Haddop', 'Python', 'Spark', 'Hadoop']) # Coleção de dados em texto tbm funciona

def funcMin(palavra):
    palavra=palavra.lower()
    return palavra

rddMinuscula = rddtexto.map(funcMin) # nao retorna um rdd para cada elemento 
rddMinuscula.take(5)

# ou 

rddMinuscula_1 = rddtexto.map(lambda x: x.lower())
rddMinuscula_1.take(5)

sc.stop()