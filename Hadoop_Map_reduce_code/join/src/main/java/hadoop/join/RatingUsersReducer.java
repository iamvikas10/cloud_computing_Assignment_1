package hadoop.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RatingUsersReducer extends Reducer <Text, Text, Text, Text>{

	public static final Log log = LogFactory.getLog(RatingUsersReducer.class);
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		
		boolean flag=false;
		String user_record[]=new String[5]; 
		
		List<Text> target = new ArrayList<Text>();
		
		
		for (Text value : values) 
		{ 
			
			String record=value.toString();
			if (!flag && record.split(",")[0].equals("User"))
			{	
				user_record=record.split(",");
				flag=true;
			}
			else
			{
				Text v= new Text(value);
				target.add(v);
			}
		}
		if(!flag)
			return;
		
		for (Text value : target) 
		{ 
			String rating_record[] = value.toString().split(",");

			String str = rating_record[2] +","+rating_record[3]+","+rating_record[4]+","+user_record[2]+","+user_record[3]+","+user_record[4]+","+user_record[5];
			context.write(new Text(rating_record[1]), new Text(str));

		}
			
	}
	
}
