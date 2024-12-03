# Contar palavras 

import pyspark
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('leitura de arquivo teste')
sc = SparkContext(conf=conf). getOrCreate()

rdd = sc.textFile('sample_data/README.md') # le arquivo de texto 

rdd.count() # conta quantas linhas tem o documento 

rdd.take(10) # retorna as 10 primeiras linhas do documento 

palavra = rdd.flatMap(lambda x: x.split(' ')) # percorre todos os elementos do RDD em buscas do espaço 
palavra.take(5) # retorna os elementos presentes até o 5º espaço

palavraMinusculaMap = palavra.map(lambda x: x.lower()) # percorre as palavras e retorna elas em minusculo 
print('Map: ', palavraMinusculaMap.take(5))

palavraMinusculaFlatMap = palavra.flatMap(lambda x: x.upper()) #percorre os caracteres e retorna em MAIÚSCULO
print('FlatMap: ', palavraMinusculaFlatMap.take(5))

palavraComecaT = palavraMinusculaMap.filter(lambda x: x.startswith('t'))
print(palavraComecaT.take(5)) # retorna a 5 palavras que inicia com a letra "t"

palavraMin2 = palavraMinusculaMap.filter(lambda x: len(x) > 2)
print(palavraMin2.take(5)) #retorna a 5 primeira palavras maior que 2 caracter

palavraChaveValor = palavraMin2.map(lambda x: (x,1)) # conceito de chave valor 
palavraChaveValor.take(5) # atribui o numero 1 a cada palavra

palavraContador = palavraChaveValor.reduceByKey(lambda x,y: x+y) 
palavraContadorOrd = palavraContador.sortByKey(ascending=-1) # ordena do maior para o menor
palavraContador.take(20)

palavraContador.saveAsTextFile('contar_palavras_out')  # salva o arquivo