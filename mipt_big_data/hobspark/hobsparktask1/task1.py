from pyspark import SparkContext, SparkConf

config = SparkConf().setAppName("my_super_app").setMaster("yarn")  # конфиг, в котором указываем название приложения и режим выполнения (local[*] для локального запуска, yarn для запуска через YARN). В систему сдаём код с мастером YARN.
sc = SparkContext(conf=config)  # создаём контекст, пользуясь конфигомfrom pyspark import SparkContext



def parse_edge(s):
  user, follower = s.split("\t")
  return (int(user), int(follower))

def step(item):
  cur_v, prev_d, next_v = item[0], item[1][0], item[1][1]
  return (next_v, cur_v)

def complete(item):
  v, old_d, new_d = item[0], item[1][0], item[1][1]
  return (v, old_d if old_d is not None else new_d)


n = 400  
end_v = 34
edges = sc.textFile("hdfs://mipt-master.atp-fivt.org:8020/data/twitter/twitter_sample.txt").map(parse_edge).cache()
forward_edges = edges.map(lambda e: (e[1], e[0])).partitionBy(n).persist()

x = 12
distances = sc.parallelize([(x,x)]).partitionBy(n)
while True:
  candidates = distances.join(forward_edges, n).map(step)
  new_distances = distances.fullOuterJoin(candidates, n).map(complete, True).distinct().persist()
  count = new_distances.filter(lambda i: i[0] == end_v).count()
  if count == 0:
    distances = new_distances
  else:
    break

answer = new_distances.collect()
dict_answer = {answer[i][0]: answer[i][1] for i in range(len(answer))}



correct_answer=[]
vert = end_v
while vert!=12:
  correct_answer.append(vert)
  vert = dict_answer[vert]
correct_answer.append(vert)
for a in correct_answer[::-1]:
  if(a!=end_v):
    print(a,end=',')
  else:
    print(a) 

