package hadoop.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RatingMoviesReducer extends Reducer <Text, Text, Text, Text>{

	public static final Log log = LogFactory.getLog(RatingMoviesReducer.class);
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		
		boolean flag=false;
		String movie_record[]=new String[5]; 
		
		List<Text> target = new ArrayList<Text>();
		
		
		for (Text value : values) 
		{ 
			
			String record=value.toString();
			if (!flag && record.split(",")[0].equals("Movie"))
			{	
				movie_record=record.split(",");
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

			String str = rating_record[2] +","+rating_record[3]+","+rating_record[4]+","+movie_record[2]+","+movie_record[3]
					+","+movie_record[4]+","+movie_record[5]
					+","+movie_record[6]+","+movie_record[7]+","+movie_record[8]
							+","+movie_record[9]+","+movie_record[10]+","+movie_record[11]
									+","+movie_record[12]+","+movie_record[13]+","+movie_record[14]+","+movie_record[15]
											+","+movie_record[16]+","+movie_record[17]+","+movie_record[18]+","+movie_record[19]
													+","+movie_record[20]+","+movie_record[21]+","+movie_record[22];
			context.write(new Text(rating_record[1]), new Text(str));

		}
			
	}
	
}
