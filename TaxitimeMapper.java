/**
 * @author: Arnold / Varun
 * @since: 2022-11-10 1:30:42 AM
 * @description: taxi time of flight by airports in asc and desc
 */
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TaxitimeMapper extends Mapper<Object, Text, Text, Text> {
	/**
	 * Taxi Time
	 */
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] infos = value.toString().split(",");
		if (!"Year".equals(infos[0])) {
			if (!"NA".equals(infos[20])) {
				context.write(new Text(infos[16]), new Text(infos[20]));
			}
			if (!"NA".equals(infos[19])) {
				context.write(new Text(infos[17]), new Text(infos[19]));
			}

		}
	}
}
