package hadoop.functions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;
import java.util.HashMap;

public class MinimumGroupByMainRating extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        HashMap<String,Integer>mapRating = new HashMap<>();
        mapRating.put("userid",0);
        mapRating.put("movieid",1);
        mapRating.put("rating",2);
        mapRating.put("timestamp",3);

        String column = args[0];
        String funColumn = args[1];
        String groupByColumns = args[2];
        String havingCondition = args[3];
        String[] arrSelectColumns = column.split(",");
        String columnNo = "";
        for(int i=0; i < arrSelectColumns.length; i++){
            columnNo += mapRating.get(arrSelectColumns[i]) + ",";
        }
        
        String[] arrGroupByCols = groupByColumns.split(",");
        String groupByColNo = "";
        for(int i =0; i < arrGroupByCols.length; i++) {
        	groupByColNo += mapRating.get(arrGroupByCols[i])+",";
        }
        
        conf.set("selectedColumn", columnNo);
        conf.set("funColumn", String.valueOf(mapRating.get(funColumn)));
        conf.set("groupByColumn", groupByColNo);
        conf.set("havingCondition", havingCondition);

        Job job = Job.getInstance(conf,"MinimumGroupByMainRating");
        job.setJobName("MinimumGroupByMainRating");
        job.setJarByClass(MinimumGroupByMainRating.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MinMapRating.class);
        job.setCombinerClass(MinReduceRating.class);
        job.setReducerClass(MinReduceRating.class);

        //Path inputFilePath = new Path(args[0]);
        Path outputPath = new Path("/cloud/output/rating");

        FileInputFormat.addInputPath(job, new Path("/cloud/input/rating.csv"));
        FileOutputFormat.setOutputPath(job, outputPath);
        
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000"),conf);
		
		// delete existing directory
		if (hdfs.exists(outputPath)) {
		    hdfs.delete(outputPath, true);
		}
        return  job.waitForCompletion(true)?0:1;
    }
    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new MinimumGroupByMainRating(), args);
        System.exit(exitCode);
    }
}
