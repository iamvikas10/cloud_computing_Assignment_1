package hadoop.functions;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MaxReduceRating extends Reducer<Text, Text, Text, Text> {
    private Integer havingCondition = 0;
    private Text result = new Text();
    private Integer resultMax = 0;
    List<Text> cache = new ArrayList<Text>();

    public void setup(Context context) throws IOException, InterruptedException{
        havingCondition = Integer.parseInt(context.getConfiguration().get("havingCondition"));
    }
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        Integer maxValue = 0;
        resultMax =0;
        cache = new ArrayList<Text>();
        for(Text val : values){
            String[] valArray = val.toString().split(",");
            int len = valArray.length;
            maxValue = Integer.parseInt(valArray[len-1]);
            if(maxValue > resultMax){
                resultMax = maxValue;
            }
            Text v = new Text(val);
            cache.add(v);
        }
        Integer tempValue = 0;
        for(Text val : cache){
            String[] temp = val.toString().split(",");
            int len = temp.length;
            tempValue = Integer.parseInt(temp[len-1]);
            if(tempValue == resultMax && tempValue > havingCondition){
                result.set(val);
                context.write(key, result);
                return;
            }
        }
    }
}
