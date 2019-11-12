package hadoop.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;

public class UsersZipcodesReducer extends Reducer <Text, Text, Text, Text>{

	public static final Log log = LogFactory.getLog(UsersZipcodesReducer.class);
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		
		boolean flag=false;
		String zipcode_record[]=new String[5]; 
		
		List<Text> target = new ArrayList<Text>();
		
		
		for (Text value : values) 
		{ 
			
			String record=value.toString();
			if (!flag && record.split(",")[0].equals("Zipcode"))
			{	
				zipcode_record=record.split(",");
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
			String user_record[] = value.toString().split(",");
//			if (user_record[0].equals("User"))
//			{
				String str = user_record[2] +","+user_record[3]+","+user_record[4]+","+zipcode_record[1]+","+zipcode_record[2]+","+zipcode_record[3]+","+zipcode_record[4];
				context.write(new Text(user_record[1]), new Text(str));
//			}
		}
			
	}
	
	
}
