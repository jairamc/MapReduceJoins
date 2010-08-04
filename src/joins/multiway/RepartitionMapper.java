package joins.multiway;

import java.io.IOException;
//import java.util.Iterator;
//import java.util.Map.Entry;

import joins.twoway.TextPair;

import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


@SuppressWarnings("deprecation")
public class RepartitionMapper extends MapReduceBase implements Mapper<Text, Text, TextPair, TextPair> {
	
	String tag = "";
	
	@Override
	public void configure(JobConf conf) {
		super.configure(conf);
		String[] tags = conf.get("tables.tags").split(";");
		String table = conf.get("map.input.file");
		for(int i=0;i<tags.length;i++)
		{
			//if(tags[i].compareTo(table)==0)
			if(table.endsWith(tags[i]))
			{
				tag = tags[i+1];
				break;
			}
		}
	}

	@Override
	public void map(Text key, Text values,
			OutputCollector<TextPair, TextPair> output, Reporter reporter) throws IOException {
		output.collect(new TextPair(key.toString(), tag), new TextPair(values.toString(), tag));
	}

}
