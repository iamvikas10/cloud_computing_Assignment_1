package hadoop.functions;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SumReduceRating extends Reducer<Text, Text, Text, Text> {
    private Integer havingCondition = 0;
    private Text result = new Text();
    private Integer resultSum = 0;
    List<Text> cache = new ArrayList<Text>();

    public void setup(Context context) throws IOException, InterruptedException{
        havingCondition = Integer.parseInt(context.getConfiguration().get("havingCondition"));
    }
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        Integer maxValue = 0;
        resultSum =0;
        cache = new ArrayList<Text>();
        for(Text val : values){
            String[] valArray = val.toString().split(",");
            int len = valArray.length;
            maxValue = Integer.parseInt(valArray[len-1]);
            resultSum = resultSum + maxValue;
//            Text v = new Text(val);
//            cache.add(v);
        }
        Text valStr = new Text();
        valStr.set(key +"," + resultSum);
        result.set(valStr);
        if(resultSum > havingCondition) {
        	context.write(key, result);
        }
    }
}
