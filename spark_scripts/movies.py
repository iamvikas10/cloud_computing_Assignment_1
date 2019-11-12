from pyspark import SparkContext, SparkConf
import sys
import time

start_time = time.time()

conf = SparkConf().setAppName("movies")
sc = SparkContext(conf = conf)

URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
fs = FileSystem.get(URI("hdfs://localhost:9000"), sc._jsc.hadoopConfiguration())

moviesRDD = sc.textFile("hdfs://localhost:9000/cloud/input/movies.csv")




mapMovies =	{
  "movieid":0,
  "title":1,
  "releasedate":2,
  "unknown": 3,
  "Action": 4,
  "Adventure": 5,
  "Animation": 6,
  "Children": 7,
  "Comedy": 8,
  "Crime": 9,
  "Documentary": 10,
  "Drama": 11,
  "Fantasy": 12,
  "Film_Noir": 13,
  "Horror": 14,
  "Musical": 15,
  "Mystery": 16,
  "Romance": 17,
  "Sci_Fi": 18,
  "Thriller": 19,
  "War": 20,
  "Western": 21,

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

    
    pcolumnsindexes  =  [mapMovies.get(x)  for x in pcolumns if mapMovies.get(x) != None]
    afcollumnidx = mapMovies.get(afcollumn)

    if len(pcolumns)==len(pcolumnsindexes):
      moviesRDDMap = moviesRDD.map(lambda rec: group_key(rec,pcolumnsindexes))
      moviesGroupByKey = moviesRDDMap.groupByKey()

      if afun == 'max':
        moviesGrouped = moviesGroupByKey.map(lambda x: (x[0],           max(int(r.split(",")[afcollumnidx]) for r in x[1] )      )   )
      elif afun == 'min':
        moviesGrouped = moviesGroupByKey.map(lambda x: (x[0],           min(int(r.split(",")[afcollumnidx]) for r in x[1] )      )   )
      elif afun == 'sum':
        moviesGrouped = moviesGroupByKey.map(lambda x: (x[0],           sum(int(r.split(",")[afcollumnidx]) for r in x[1] )      )   )
      elif afun == 'count':
        moviesGrouped = moviesGroupByKey.map(lambda x: (x[0],           len(list(x[1]))       )   )
      
      moviesGF =  moviesGrouped.filter(lambda line: int(line[1]) > int(val) )

      fs.delete(Path('/cloud/output/spark/movies') ,True)
      moviesGF.coalesce(1, shuffle = True).saveAsTextFile("hdfs://localhost:9000/cloud/output/spark/movies")

      execution_time = time.time() - start_time
      f = open("extime.txt", "w")
      f.write(str(execution_time))
      f.close()

      #for i in moviesGF.collect(): print(i)





    else:
      print("invalid query")

if __name__== "__main__":
  main()
