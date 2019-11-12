package hadoop.join;

import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MoviesRating {

	
	public static void main(String[] args) throws Exception{

		Configuration conf = new Configuration();
		
		HashMap<String, Integer> mapRating = new HashMap<>(); 
		mapRating.put("userid",0);
		mapRating.put("movieid",1);
		mapRating.put("rating",2);
		mapRating.put("timestamp",3);
		
		HashMap<String, Integer> mapMovies = new HashMap<>(); 
		mapMovies.put("movieid",0);
		mapMovies.put("title",1);
		mapMovies.put("releasedate",2);
		mapMovies.put("unknown",3);
		mapMovies.put("Action",4);
		mapMovies.put("Adventure",5);
		mapMovies.put("Animation",6);
		mapMovies.put("Children",7);
		mapMovies.put("Comedy",8);
		mapMovies.put("Crime",9);
		mapMovies.put("Documentary",10);
		mapMovies.put("Drama",11);
		mapMovies.put("Fantasy",12);
		mapMovies.put("Film_Noir",13);
		mapMovies.put("Horror",14);
		mapMovies.put("Musical",15);
		mapMovies.put("Mystery",16);
		mapMovies.put("Romance",17);
		mapMovies.put("Sci_Fi",18);
		mapMovies.put("Thriller",19);
		mapMovies.put("War",20);
		mapMovies.put("Western",21);
		
		
		
		String attribute="DEFAULT";
		String val = "DEFAULT";
		if(args.length==2)
		{
			attribute=args[0];
			val=args[1];
		}

		if(mapRating.containsKey(attribute))
		{
			conf.set("table1","rating");
			conf.set("attributeIndex1", String.valueOf(mapRating.get(attribute)));
			conf.set("val1", val);
		}
		
		if(mapMovies.containsKey(attribute))
		{
			conf.set("table2","movies");
			conf.set("attributeIndex2", String.valueOf(mapMovies.get(attribute)));
			conf.set("val2", val);
		}
		
		Job job = new Job(conf, "Rating Movies");
		 job.setJarByClass(MoviesRating.class);
		 job.setReducerClass(RatingMoviesReducer.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
		 
		 
		 
		 MultipleInputs.addInputPath(job, new Path("/cloud/input/rating.csv"),TextInputFormat.class, RatingMapperMR.class);
		 MultipleInputs.addInputPath(job, new Path("/cloud/input/movies.csv"),TextInputFormat.class, MoviesMapperMR.class);
		 Path outputPath = new Path("/cloud/output/moviesrating");
		  
		 FileOutputFormat.setOutputPath(job, outputPath);
		 
		 FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000"),conf);
			
			// delete existing directory
			if (hdfs.exists(outputPath)) {
			    hdfs.delete(outputPath, true);
			}
		 
		 
		 
		 
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	
	
}
