package hadoop.join;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UsersMapperRU extends Mapper <Object, Text, Text, Text>{

	
	private String table2 = null;
	private String attributeIndex2 = null;
	private String val2 = null;
	
	public static final Log log = LogFactory.getLog(UsersMapperRU.class);

	public void setup(Context context) throws IOException, InterruptedException {
		table2 = context.getConfiguration().get("table2");
		attributeIndex2 = context.getConfiguration().get("attributeIndex2");
		val2 = context.getConfiguration().get("val2");
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
			String record = value.toString();
			String[] parts = record.split(",");
//			
//			log.info("table2 ZipcodesMapper:"+table2);
//			log.info("attributeIndex ZipcodesMapper:"+attributeIndex2);
//			log.info("val ZipcodesMapper:" + val2);
			
			
			
			if(table2 != null)
			{
				int index=Integer.valueOf(attributeIndex2);
				String str=parts[index];
				str=str.replace("\"", "");
				if(str.equals(val2))
				{
					context.write(new Text(parts[0]), new Text("User," + parts[0] +","+ parts[1] +","+ parts[2] +","+ parts[3]+","+ parts[4]));
				}
			}	
			else
				context.write(new Text(parts[0]), new Text("User," + parts[0] +","+ parts[1] +","+ parts[2] +","+ parts[3]+","+ parts[4]));
			
			
	}
	
	
}
