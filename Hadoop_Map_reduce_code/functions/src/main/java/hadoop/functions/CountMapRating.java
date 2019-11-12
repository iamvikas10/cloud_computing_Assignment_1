package hadoop.functions;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;

@SuppressWarnings("unused")
public class CountMapRating extends Mapper<LongWritable, Text, Text, IntWritable> {
	private String groupByColumnConf = null;
	//  private String funColumnConf = null;
	//  private String selectColumnsConf = null;
	  //private String groupByNo = null;
	private Text groupByColumn = new Text();
	private IntWritable count = new IntWritable();
	  //public Text output = new Text();
	
	public void setup(Context context) throws IOException, InterruptedException{
	      groupByColumnConf = context.getConfiguration().get("groupByColumn");
	//    funColumnConf = context.getConfiguration().get("funColumn");
	//    selectColumnsConf = context.getConfiguration().get("selectedColumn");
	}
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] row = value.toString().split(",");
        //int x = Integer.parseInt((groupByColumnConf));
        //groupByColumn.set(row[x]);
        String[] groupColumnNo = groupByColumnConf.split(",");
        int [] arrGroup;
        arrGroup = new int[5];
        for(int i =0; i < groupColumnNo.length; i++) {
        	arrGroup[i] = Integer.parseInt(groupColumnNo[i]);
        }
        int lenGroupBy = groupColumnNo.length;
        if(lenGroupBy == 5) {
        	groupByColumn.set(row[arrGroup[0]]+ "," + row[arrGroup[1]]+ "," + row[arrGroup[2]]+ "," + row[arrGroup[3]]+ "," + row[arrGroup[4]]);
        }
        else if(lenGroupBy == 4) {
        	groupByColumn.set(row[arrGroup[0]]+ "," + row[arrGroup[1]]+ "," + row[arrGroup[2]]+ "," + row[arrGroup[3]]);
        }
        else if(lenGroupBy == 3) {
        	groupByColumn.set(row[arrGroup[0]]+ "," + row[arrGroup[1]]+ "," + row[arrGroup[2]]);
        }
        else if(lenGroupBy == 2) {
        	groupByColumn.set(row[arrGroup[0]]+ "," + row[arrGroup[1]]);
        }
        else if(lenGroupBy == 1) {
        	groupByColumn.set(row[arrGroup[0]]);

        }
        count.set(1);
        context.write(groupByColumn, count);
    }
}
