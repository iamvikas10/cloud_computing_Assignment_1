package hadoop.functions;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountReduceZipcodes extends Reducer<Text, IntWritable, Text, IntWritable> {
    private Integer havingCondition = 0;
    private IntWritable result = new IntWritable();
    // private Integer resultCount = 0;
    //List<Text> cache = new ArrayList<Text>();

    public void setup(Context context) throws IOException, InterruptedException{
        havingCondition = Integer.parseInt(context.getConfiguration().get("havingCondition"));
    }
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        Integer count = 0;
        for(IntWritable value : values){
            count += value.get();
        }
        if(count > havingCondition){
            result.set(count);
            context.write(key, result);
        }

    }
}
