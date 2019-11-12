package hadoop.join;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.mapreduce.Mapper.Context;

public class UsersMapper extends Mapper <Object, Text, Text, Text>{

	private String table1 = null;
	private String attributeIndex1 = null;
	private String val1 = null;
	
	public static final Log log = LogFactory.getLog(UsersMapper.class);

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
//			if(table1.equals("users"))
			{
				int index=Integer.valueOf(attributeIndex1);
				String str=parts[index];
				str=str.replace("\"", "");
//				log.info(str+" "+val1);
				if(str.equals(val1))
				{
//					log.info("Writing");
					context.write(new Text(parts[4]), new Text("User," + parts[0] +","+ parts[1] +","+ parts[2] +","+ parts[3] +","+ parts[4]));
				}
			}
			else
				context.write(new Text(parts[4]), new Text("User," + parts[0] +","+ parts[1] +","+ parts[2] +","+ parts[3] +","+ parts[4]));
			
	}
	
}
