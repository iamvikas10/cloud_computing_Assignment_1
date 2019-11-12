package hadoop.functions;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MinReduceUsers extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();
    private Integer resultMin = Integer.MAX_VALUE;
    List<Text> cache = new ArrayList<Text>();
    private Integer havingCondition =0;
    public void setup(Context context) throws IOException, InterruptedException{
        havingCondition = Integer.parseInt(context.getConfiguration().get("havingCondition"));
    }
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        Integer minAge = 0;
        resultMin = Integer.MAX_VALUE;
        cache = new ArrayList<Text>();
        for(Text val : values){
            String[] valArray = val.toString().split(",");
            int len = valArray.length;
            minAge = Integer.parseInt(valArray[len-1]);
            if(minAge < resultMin){
                resultMin = minAge;
            }
            Text v = new Text(val);
            cache.add(v);
        }
        Integer tempAge = 0;
        for(Text val : cache){
            String[] temp = val.toString().split(",");
            int len = temp.length;
            tempAge = Integer.parseInt(temp[len-1]);
            if(tempAge == resultMin && tempAge > havingCondition){
                result.set(val);
                context.write(key, result);
                return;
            }
        }

    }
}
