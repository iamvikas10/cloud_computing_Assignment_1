from pyspark import SparkContext, SparkConf
import sys
import time

start_time = time.time()

conf = SparkConf().setAppName("userszipcodes")
sc = SparkContext(conf = conf)

URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
fs = FileSystem.get(URI("hdfs://localhost:9000"), sc._jsc.hadoopConfiguration())


usersRDD = sc.textFile("hdfs://localhost:9000/cloud/input/users.csv")
usersRDDMap = usersRDD.map(lambda rec: (rec.split(",")[4], rec))

zipcodesRDD = sc.textFile("hdfs://localhost:9000/cloud/input/zipcodes.csv")
zipcodesRDDMap = zipcodesRDD.map(lambda rec: (rec.split(",")[0], rec))

userszipcodesRDDMap=usersRDDMap.join(zipcodesRDDMap)
userszipcodesRDDMapF = userszipcodesRDDMap

mapUsers =	{
  "userid": 0,
  "age": 1,
  "gender": 2,
  "occupation": 3,
  "zipcode": 4,
}

mapZipcodes = {
  "zipcode": 0,
  "zipcodetype": 1,
  "city": 2,
  "state": 3,
}

def main():
  arguments = sys.argv[1:]
  count = len(arguments)
  if count == 2:
    attribute = arguments[0]
    val = arguments[1]
    attributeIndexUsers = mapUsers.get(attribute)
    attributeIndexZipcodes = mapZipcodes.get(attribute)
    if attributeIndexUsers != None:
      if attribute == 'userid' or attribute == 'age':
        userszipcodesRDDMapF = userszipcodesRDDMap.filter(lambda row: int(row[1][0].split(",")[attributeIndexUsers]) == int(val))
      else:
        userszipcodesRDDMapF = userszipcodesRDDMap.filter(lambda row: (val in row[1][0].split(",")[attributeIndexUsers]))
    elif attributeIndexZipcodes != None:
      userszipcodesRDDMapF = userszipcodesRDDMap.filter(lambda row: (val in row[1][1].split(",")[attributeIndexZipcodes]))
    else:
      userszipcodesRDDMapF = userszipcodesRDDMap
  else:
    userszipcodesRDDMapF = userszipcodesRDDMap
  
  
  fs.delete(Path('/cloud/output/spark/userszipcodes') ,True)
  userszipcodesRDDMapF.coalesce(1, shuffle = True).saveAsTextFile("hdfs://localhost:9000/cloud/output/spark/userszipcodes")


  execution_time = time.time() - start_time
  f = open("extime.txt", "w")
  f.write(str(execution_time))
  f.close()

if __name__== "__main__":
  main()
