import os 
from pyspark.sql import SparkSession

# Codigo padrão para teste 
# Imprimi variáveis de um ambiente específico 

variables = [
    'JAVA_HOME',
    'HADOOP_HOME',
    'SPARK_HOME',
    'SPARK_DIST_CLASSPATH',
    'PYSPARK_PYTHON',
    'PYSPARK_DRIVER_PYHTON'
]
print('Variáveis de Ambiente')

#for var in variables:
#    value = os.environ.get(var, 'Não Definida:')
#    print(f'{var}={value}')

# Iniciar a sessão Spark 
spark = SparkSession.builder.getOrCreate()
print(f'Versão do Spark:{spark.version}')

