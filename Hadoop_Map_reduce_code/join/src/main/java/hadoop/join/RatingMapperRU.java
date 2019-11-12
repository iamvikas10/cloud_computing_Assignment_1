package hadoop.join;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingMapperRU extends Mapper <Object, Text, Text, Text>{

	private String table1 = null;
	private String attributeIndex1 = null;
	private String val1 = null;
	
	public static final Log log = LogFactory.getLog(RatingMapperRU.class);

	public void setup(Context context) throws IOException, InterruptedException {
		table1 = context.getConfiguration().get("table1");
		attributeIndex1 = context.getConfiguration().get("attributeIndex1");
		val1 = context.getConfiguration().get("val1");
	}
	
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
			String record = value.toString();
			String[] parts = record.split(",");
			
//			log.info("table UsersMapper:"+table1);
//			log.info("attributeIndex UsersMapper:"+attributeIndex1);
//			log.info("val UsersMapper:" + val1);
			
			
			if(table1!=null)
			{
				int index=Integer.valueOf(attributeIndex1);
				String str=parts[index];
				str=str.replace("\"", "");
				if(str.equals(val1))
				{
					context.write(new Text(parts[0]), new Text("Rating," + parts[0] +","+ parts[1] +","+ parts[2] +","+ parts[3]));
				}
			}
			else
				context.write(new Text(parts[0]), new Text("Rating," + parts[0] +","+ parts[1] +","+ parts[2] +","+ parts[3]));
			
	}
	
}
