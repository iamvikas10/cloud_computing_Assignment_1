package hadoop.functions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;
import java.util.HashMap;

public class CountGroupByMainMovies extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
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

        String column = args[0];
        //String funColumn = args[1];
        String groupByColumns = args[1];
        String havingCondition = args[2];
        String[] arrSelectColumns = column.split(",");
        String columnNo = "";
        for(int i=0; i < arrSelectColumns.length; i++){
            columnNo += mapMovies.get(arrSelectColumns[i]) + ",";
        }
        String[] arrGroupByCols = groupByColumns.split(",");
        String groupByColNo = "";
        for(int i =0; i < arrGroupByCols.length; i++) {
        	groupByColNo += mapMovies.get(arrGroupByCols[i])+",";
        }
        conf.set("selectedColumn", columnNo);
        //conf.set("funColumn", funColumn);
        conf.set("groupByColumn", groupByColNo);
        conf.set("havingCondition", havingCondition);



        Job job = Job.getInstance(conf, "CountGroupByMainMovies");
        job.setJobName("CountGroupByMainMovies");
        job.setJarByClass(CountGroupByMainMovies.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(CountMapMovies.class);
        job.setCombinerClass(CountReduceMovies.class);
        job.setReducerClass(CountReduceMovies.class);

        //Path inputFilePath = new Path(args[0]);
        Path outputPath = new Path("/cloud/output/movies");

        FileInputFormat.addInputPath(job, new Path("/cloud/input/movies.csv"));
        FileOutputFormat.setOutputPath(job, outputPath);
        
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000"),conf);
		
		// delete existing directory
		if (hdfs.exists(outputPath)) {
		    hdfs.delete(outputPath, true);
		}
        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String args[]) throws Exception{
        int exitCode = ToolRunner.run(new CountGroupByMainMovies(), args);
        System.exit(exitCode);
    }
}
