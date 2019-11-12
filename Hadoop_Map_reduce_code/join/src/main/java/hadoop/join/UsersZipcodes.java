package hadoop.join;

import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class UsersZipcodes {

	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		 
		HashMap<String, Integer> mapUsers = new HashMap<>(); 
		mapUsers.put("userid",0);
		mapUsers.put("age",1);
		mapUsers.put("gender",2);
		mapUsers.put("occupation",3);
		mapUsers.put("zipcode",4);
		
		
		HashMap<String, Integer> mapZipcodes = new HashMap<>(); 
		mapZipcodes.put("zipcode",0);
		mapZipcodes.put("zipcodetype",1);
		mapZipcodes.put("city",2);
		mapZipcodes.put("state",3);
		
		String attribute="DEFAULT";
		String val = "DEFAULT";
		if(args.length==2)
		{
			attribute=args[0];
			val=args[1];
		}

		if(mapUsers.containsKey(attribute))
		{
			conf.set("table1","users");
			conf.set("attributeIndex1", String.valueOf(mapUsers.get(attribute)));
			conf.set("val1", val);
		}
		
		if(mapZipcodes.containsKey(attribute))
		{
			conf.set("table2","zipcodes");
			conf.set("attributeIndex2", String.valueOf(mapZipcodes.get(attribute)));
			conf.set("val2", val);
		}
		
		Job job = new Job(conf, "Users Zipcodes");
		 job.setJarByClass(UsersZipcodes.class);
		 job.setReducerClass(UsersZipcodesReducer.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
		 
		 
		 
		 MultipleInputs.addInputPath(job, new Path("/cloud/input/users.csv"),TextInputFormat.class, UsersMapper.class);
		 MultipleInputs.addInputPath(job, new Path("/cloud/input/zipcodes.csv"),TextInputFormat.class, ZipcodesMapper.class);
		 Path outputPath = new Path("/cloud/output/userszipcodes");
		  
		 FileOutputFormat.setOutputPath(job, outputPath);
		 
		 FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000"),conf);
			
			// delete existing directory
			if (hdfs.exists(outputPath)) {
			    hdfs.delete(outputPath, true);
			}
		 
		 
		 
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
		 	
	}

}
