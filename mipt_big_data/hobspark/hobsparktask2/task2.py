import pandas as pd
import numpy as np
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

  
spark = SparkSession.builder.appName("example").master("yarn").getOrCreate()

end_v = 34
n = 400  # number of partitions

schema = StructType([ \
    StructField("user_id", IntegerType()), \
    StructField("follower_id", IntegerType())
  ])

schema1 = StructType([ \
    StructField("user_id1", IntegerType()), \
    StructField("follower_id1", IntegerType())
  ])


n=400
ds = spark.read.format("csv").schema(schema).option("sep","\t").load('hdfs://mipt-master.atp-fivt.org:8020/data/twitter/twitter_sample.txt').repartition(n).persist()

end_v = 34
x = 12
distances = spark.createDataFrame(data=[(x,x)], schema = schema1)


while True:
  candidates = distances.join(ds, distances.user_id1 == ds.follower_id)
  candidates = candidates.select(candidates.user_id, candidates.follower_id)\
  .withColumnRenamed("user_id","user_id1").withColumnRenamed("follower_id","follower_id1")
  distances = distances.union(candidates).persist()
  window = Window.partitionBy("user_id1").orderBy('tiebreak')
  
  distances = distances.withColumn('tiebreak', monotonically_increasing_id())\
  .withColumn('rank', rank().over(window))\
  .filter(col('rank') == 1).drop('rank','tiebreak')


  if len(candidates.repartition(1).where(candidates.user_id1==end_v).head(1))!=0 :
    break



result_pdf = distances.repartition(1).select("*").toPandas()
x = result_pdf['user_id1']
y = result_pdf['follower_id1']
dict_answer = {result_pdf['user_id1'][i]: result_pdf['follower_id1'][i] for i in range(len(result_pdf['user_id1']))}

correct_answer=[]
vert = end_v
while vert!=12:
  correct_answer.append(vert)
  vert = dict_answer[vert]
correct_answer.append(vert)
for a in correct_answer[::-1]:
  if(a!=end_v):
    print(a, end=',')
  else:
    print(a) 

