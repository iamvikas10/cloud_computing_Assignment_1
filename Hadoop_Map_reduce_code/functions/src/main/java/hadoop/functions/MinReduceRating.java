package hadoop.functions;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MinReduceRating extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();
    private Integer resultMin = Integer.MAX_VALUE;
    List<Text> cache = new ArrayList<Text>();
    private Integer havingCondition =0;
    public void setup(Context context) throws IOException, InterruptedException{
        havingCondition = Integer.parseInt(context.getConfiguration().get("havingCondition"));
    }
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        Integer minValue = 0;
        resultMin = Integer.MAX_VALUE;
        cache = new ArrayList<Text>();
        for(Text val : values){
            String[] valArray = val.toString().split(",");
            int len = valArray.length;
            minValue = Integer.parseInt(valArray[len-1]);
            if(minValue < resultMin){
                resultMin = minValue;
            }
            Text v = new Text(val);
            cache.add(v);
        }
        Integer tempValue = 0;
        for(Text val : cache){
            String[] temp = val.toString().split(",");
            int len = temp.length;
            tempValue = Integer.parseInt(temp[len-1]);
            if(tempValue == resultMin && tempValue > havingCondition){
                result.set(val);
                context.write(key, result);
                return;
            }
        }

    }
}
