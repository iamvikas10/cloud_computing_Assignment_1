from pyspark import SparkContext, SparkConf
import sys
import time

start_time = time.time()


conf = SparkConf().setAppName("moviesrating")
sc = SparkContext(conf = conf)

URI = sc._gateway.jvm.java.net.URI
Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
fs = FileSystem.get(URI("hdfs://localhost:9000"), sc._jsc.hadoopConfiguration())


ratingRDD = sc.textFile("hdfs://localhost:9000/cloud/input/rating.csv")
ratingRDDMap = ratingRDD.map(lambda rec: (rec.split(",")[1], rec))

moviesRDD = sc.textFile("hdfs://localhost:9000/cloud/input/movies.csv")
moviesRDDMap = moviesRDD.map(lambda rec: (rec.split(",")[0], rec))

ratingmoviesRDDMap=ratingRDDMap.join(moviesRDDMap)
ratingmoviesRDDMapF = ratingmoviesRDDMap

mapRating = {
  "userid": 0,
  "movieid": 1,
  "rating": 2,
  "timestamp": 3,
}

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

def main():
  
  arguments = sys.argv[1:]
  count = len(arguments)
  if count == 2:
    attribute = arguments[0]
    val = arguments[1]
    attributeIndexRating = mapRating.get(attribute)
    attributeIndexMovies = mapMovies.get(attribute)
    if attributeIndexRating != None:
      ratingmoviesRDDMapF = ratingmoviesRDDMap.filter(lambda row: int(row[1][0].split(",")[attributeIndexRating]) == int(val))
    elif attributeIndexMovies != None:
      if attribute == 'title' or attribute == 'releasedate':
        ratingmoviesRDDMapF = ratingmoviesRDDMap.filter(lambda row: (val in row[1][1].split(",")[attributeIndexMovies]))
      else:
        ratingmoviesRDDMapF = ratingmoviesRDDMap.filter(lambda row: int(row[1][1].split(",")[attributeIndexMovies]) == int(val))
    else:
      ratingmoviesRDDMapF = ratingmoviesRDDMap
  else:
    ratingmoviesRDDMapF = ratingmoviesRDDMap
  
  #for i in ratingmoviesRDDMapF.collect():
  #  print(i)
  
  fs.delete(Path('/cloud/output/spark/moviesrating') ,True)
  
  ratingmoviesRDDMapF.coalesce(1, shuffle = True).saveAsTextFile("hdfs://localhost:9000/cloud/output/spark/moviesrating")
  
  execution_time = time.time() - start_time
  f = open("extime.txt", "w")
  f.write(str(execution_time))
  f.close()

if __name__== "__main__":
  main()
