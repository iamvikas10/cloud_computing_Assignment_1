from pyspark import SparkContext, SparkConf
import sys
import time

start_time = time.time()

conf = SparkConf().setAppName("zipcodes")
sc = SparkContext(conf = conf)

URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
fs = FileSystem.get(URI("hdfs://localhost:9000"), sc._jsc.hadoopConfiguration())

zipcodesRDD = sc.textFile("hdfs://localhost:9000/cloud/input/zipcodes.csv")




mapZipcodes = {
  "zipcode": 0,
  "zipcodetype": 1,
  "city": 2,
  "state": 3,
}

def group_key(rec,pindexes):
  
  key=""  
  rec_list=rec.split(',')
  for i in pindexes:
    if key != "":
      key=key +','+ rec_list[i] 
    else:
      key=rec_list[i]
  return key,rec

def main():
  arguments = sys.argv[1:]
  count = len(arguments)
  if count == 4:
    pcolumns = arguments[0].split(',')
    afun = arguments[1]
    afcollumn = arguments[2]

    val = arguments[3]

    
    pcolumnsindexes  =  [mapZipcodes.get(x)  for x in pcolumns if mapZipcodes.get(x) != None]
    afcollumnidx = mapZipcodes.get(afcollumn)

    if len(pcolumns)==len(pcolumnsindexes):
      zipcodesRDDMap = zipcodesRDD.map(lambda rec: group_key(rec,pcolumnsindexes))
      zipcodesGroupByKey = zipcodesRDDMap.groupByKey()

      if afun == 'max':
        zipcodesGrouped = zipcodesGroupByKey.map(lambda x: (x[0],           max(int(r.split(",")[afcollumnidx]) for r in x[1] )      )   )
      elif afun == 'min':
        zipcodesGrouped = zipcodesGroupByKey.map(lambda x: (x[0],           min(int(r.split(",")[afcollumnidx]) for r in x[1] )      )   )
      elif afun == 'sum':
        zipcodesGrouped = zipcodesGroupByKey.map(lambda x: (x[0],           sum(int(r.split(",")[afcollumnidx]) for r in x[1] )      )   )
      elif afun == 'count':
        zipcodesGrouped = zipcodesGroupByKey.map(lambda x: (x[0],           len(list(x[1]))       )   )
      
      zipcodesGF =  zipcodesGrouped.filter(lambda line: int(line[1]) > int(val) )

      fs.delete(Path('/cloud/output/spark/zipcodes') ,True)
      zipcodesGF.coalesce(1, shuffle = True).saveAsTextFile("hdfs://localhost:9000/cloud/output/spark/zipcodes")

      execution_time = time.time() - start_time
      f = open("extime.txt", "w")
      f.write(str(execution_time))
      f.close()

      #for i in zipcodesGF.collect(): print(i)





    else:
      print("invalid query")

if __name__== "__main__":
  main()
