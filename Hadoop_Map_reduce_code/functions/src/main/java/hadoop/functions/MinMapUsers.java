package hadoop.functions;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MinMapUsers extends Mapper<LongWritable, Text, Text, Text> {
    private String groupByColumnConf = null;
    private String funColumnConf = null;
    private String selectColumnsConf = null;
    private Text groupByColumn = new Text();
    private Integer minValue;
    public Text output = new Text();

    public void setup(Context context) throws IOException, InterruptedException{
        groupByColumnConf = context.getConfiguration().get("groupByColumn");
        funColumnConf = context.getConfiguration().get("funColumn");
        selectColumnsConf = context.getConfiguration().get("selectedColumn");
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] row = value.toString().split(",");
//        int x = Integer.parseInt((groupByColumnConf));
//        groupByColumn.set(row[x]);
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
        
        minValue = Integer.parseInt(row[Integer.parseInt(funColumnConf)]);

        String[] selectedColumnNo = selectColumnsConf.split(",");
        int [] arr;
        arr = new int[5];
        for(int i = 0; i < selectedColumnNo.length; i ++){
            arr[i] = Integer.parseInt(selectedColumnNo[i]);
        }
        int len = selectedColumnNo.length;

        if(len == 5){
            context.write(groupByColumn, new Text(row[arr[0]] + ","+ row[arr[1]] + "," + row[arr[2]] + ","+ row[arr[3]] +","+row[arr[4]]+ ","+ minValue));
        }
        else if(len ==4){
            context.write(groupByColumn, new Text(row[arr[0]] + ","+ row[arr[1]] + "," + row[arr[2]] + ","+ row[arr[3]] +","+ minValue));
        }
        else if(len == 3){
            context.write(groupByColumn, new Text(row[arr[0]] + ","+ row[arr[1]] + "," + row[arr[2]]  +","+ minValue));
        }
        else if(len == 2){
            context.write(groupByColumn, new Text(row[arr[0]] + ","+ row[arr[1]] + "," + minValue));
            System.out.println("just Testing");
        }
        else if(len == 1){
            context.write(groupByColumn, new Text(row[arr[0]] + ","+  minValue));
        }
    }
}
