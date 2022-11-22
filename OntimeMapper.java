/**
 * @author: Arnold / Varun
 * @since: 2022-11-10 1:30:42 AM
 * @description: the 3 airlines with the highest and lowest probability on time
 */
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OntimeMapper extends Mapper<Object, Text, Text, Text> {
	/**
	 * on time carrier
	 */
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] infos = value.toString().split(",");
		if (!"Year".equals(infos[0])) {
			String normal = "0";
			if(!"NA".equals(infos[14])){
				if (Integer.parseInt(infos[14]) <= 10 ) {
					normal = "1";
				}
			context.write(new Text(infos[8]), new Text(normal));
			}
			
		}
	}
}
