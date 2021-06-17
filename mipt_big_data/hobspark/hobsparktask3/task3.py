from pyspark import SparkContext, SparkConf
import re
import math


def preoroc(line):
    return_words=[]
    article_id, text = line.strip().split('\t', 1)
    words = re.split("\W*\s+\W*", text, flags=re.UNICODE)
    for word in words:
        word = word.lower()
        word = re.sub("^\W+|\W+$", "", word)
        if word in stop_words or word=='':
          continue
        else:
          return_words.append(word)
    return return_words


def get_prob(item):
  x, y = item[0], item[1]
  y = int(y)/broadcast_count_bigrams.value
  return (x, y)

def get_prob_alone(item):
  x, y = item[0], item[1]
  y = int(y)/broadcast_count_alone.value
  return (x, y)

def getNPMI(item):
  p_a = prob.value[item[0][0]]
  p_b = prob.value[item[0][1]]
  p_ab = item[1]
  
  pmi = math.log(p_ab/(p_a*p_b))
  ex = math.log(p_ab)
  answer = -pmi/ex
  return (item[0], answer)

stop_words = set()
with open("stop_words_en-xpo6.txt") as fd:
    for line in fd:
        stop_words.add(line.strip())

config = SparkConf().setAppName("my_super_app").setMaster("yarn") 
sc = SparkContext(conf=config)



n=400
rdd = sc.textFile("hdfs://mipt-master.atp-fivt.org:8020/data/wiki/en_articles_part/articles-part").map(preoroc).flatMap(lambda xs: (tuple(x) for x in zip(xs, xs[1:]))).cache()
bigrams = rdd.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).partitionBy(n).cache()

count_bigrams = bigrams.count()
broadcast_count_bigrams = sc.broadcast(count_bigrams)
new_bigrams = bigrams.filter(lambda x: x[1] >= 500)
new_bigrams=new_bigrams.map(get_prob).cache()

rdd1 = sc.textFile("hdfs://mipt-master.atp-fivt.org:8020/data/wiki/en_articles_part/articles-part").flatMap(preoroc).cache()
alone = rdd1.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).partitionBy(n).cache()

count_alone = alone.count()
broadcast_count_alone = sc.broadcast(count_alone)
alone=alone.map(get_prob_alone).cache()
alone_broadcast = alone.collect()
dictionary = {k:v for k,v in alone_broadcast}
prob = sc.broadcast(dictionary)
new_bigrams = new_bigrams.map(getNPMI)
new_bigrams = new_bigrams.sortBy(lambda a: -a[1])

example = new_bigrams.take(39)
for item in example:
  string = item[0][0]+'_'+item[0][1]
  print(string)


