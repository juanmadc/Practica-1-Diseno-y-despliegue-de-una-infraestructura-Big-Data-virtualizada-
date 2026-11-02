"""Contador de palabras: lee de HDFS, escribe en HDFS y muestra qué nodos trabajan.

Uso (desde el master, con docker exec):
  spark-submit /jobs/wordcount.py hdfs:///data/entrada.txt hdfs:///data/salida
"""
import socket
import sys
from collections import Counter

from pyspark.sql import SparkSession

entrada, salida = sys.argv[1], sys.argv[2]

spark = SparkSession.builder.appName("wordcount-hdfs").getOrCreate()
sc = spark.sparkContext

# Varias particiones: así las tareas se reparten entre ambos workers
lineas = sc.textFile(entrada).repartition(4)
cuentas = (
    lineas.flatMap(lambda l: l.lower().split())
    .map(lambda w: (w, 1))
    .reduceByKey(lambda a, b: a + b)
)
cuentas.saveAsTextFile(salida)

# Demostración del reparto: tareas ejecutadas por cada nodo
por_nodo = (
    sc.parallelize(range(8), 8)
    .map(lambda _: socket.gethostname())
    .collect()
)
print("Tareas por nodo:", dict(Counter(por_nodo)))
print("Palabras distintas:", cuentas.count())

spark.stop()
