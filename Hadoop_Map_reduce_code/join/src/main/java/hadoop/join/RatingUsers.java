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

public class RatingUsers {

	public static void main(String[] args) throws Exception{

		Configuration conf = new Configuration();
		
		HashMap<String, Integer> mapRating = new HashMap<>(); 
		mapRating.put("userid",0);
		mapRating.put("movieid",1);
		mapRating.put("rating",2);
		mapRating.put("timestamp",3);
		
		HashMap<String, Integer> mapUsers = new HashMap<>(); 
		mapUsers.put("userid",0);
		mapUsers.put("age",1);
		mapUsers.put("gender",2);
		mapUsers.put("occupation",3);
		mapUsers.put("zipcode",4);
		
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
		
		if(mapUsers.containsKey(attribute))
		{
			conf.set("table2","users");
			conf.set("attributeIndex2", String.valueOf(mapUsers.get(attribute)));
			conf.set("val2", val);
		}
		
		Job job = new Job(conf, "Rating Users");
		 job.setJarByClass(RatingUsers.class);
		 job.setReducerClass(RatingUsersReducer.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
		 
		 
		 
		 MultipleInputs.addInputPath(job, new Path("/cloud/input/rating.csv"),TextInputFormat.class, RatingMapperRU.class);
		 MultipleInputs.addInputPath(job, new Path("/cloud/input/users.csv"),TextInputFormat.class, UsersMapperRU.class);
		 Path outputPath = new Path("/cloud/output/ratingusers");
		  
		 FileOutputFormat.setOutputPath(job, outputPath);
		 
		 FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000"),conf);
			
			// delete existing directory
			if (hdfs.exists(outputPath)) {
			    hdfs.delete(outputPath, true);
			}
		 
		 
		 
		 
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
