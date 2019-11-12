from pyspark import SparkContext, SparkConf
import sys
import time

start_time = time.time()

conf = SparkConf().setAppName("ratingusers")
sc = SparkContext(conf = conf)

URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
fs = FileSystem.get(URI("hdfs://localhost:9000"), sc._jsc.hadoopConfiguration())


ratingRDD = sc.textFile("hdfs://localhost:9000/cloud/input/rating.csv")
ratingRDDMap = ratingRDD.map(lambda rec: (rec.split(",")[0], rec))

usersRDD = sc.textFile("hdfs://localhost:9000/cloud/input/users.csv")
usersRDDMap = usersRDD.map(lambda rec: (rec.split(",")[0], rec))

ratingusersRDDMap=ratingRDDMap.join(usersRDDMap)
ratingusersRDDMapF = ratingusersRDDMap

mapRating = {
  "userid": 0,
  "movieid": 1,
  "rating": 2,
  "timestamp": 3,
}

mapUsers =	{
  "userid": 0,
  "age": 1,
  "gender": 2,
  "occupation": 3,
  "zipcode": 4,
}



def main():
  arguments = sys.argv[1:]
  count = len(arguments)
  if count == 2:
    attribute = arguments[0]
    val = arguments[1]
    attributeIndexRating = mapRating.get(attribute)
    attributeIndexUsers = mapUsers.get(attribute)
    if attributeIndexRating != None:
      ratingusersRDDMapF = ratingusersRDDMap.filter(lambda row: int(row[1][0].split(",")[attributeIndexRating]) == int(val))
    elif attributeIndexUsers != None:
      if attribute == 'userid' or attribute == 'age':
        ratingusersRDDMapF = ratingusersRDDMap.filter(lambda row: int(row[1][1].split(",")[attributeIndexUsers]) == int(val))
      else:
        ratingusersRDDMapF = ratingusersRDDMap.filter(lambda row: (val in row[1][1].split(",")[attributeIndexUsers]))
    else:
      ratingusersRDDMapF = ratingusersRDDMap
  else:
    ratingusersRDDMapF = ratingusersRDDMap
  
  #for i in ratingusersRDDMapF.collect():
  #  print(i)
  
  fs.delete(Path('/cloud/output/spark/ratingusers') ,True)
  ratingusersRDDMapF.coalesce(1, shuffle = True).saveAsTextFile("hdfs://localhost:9000/cloud/output/spark/ratingusers")

  execution_time = time.time() - start_time
  f = open("extime.txt", "w")
  f.write(str(execution_time))
  f.close()  

if __name__== "__main__":
  main()
