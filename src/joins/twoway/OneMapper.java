package joins.twoway;

import java.io.IOException;
//import java.util.Iterator;
//import java.util.Map.Entry;

import joins.twoway.TextPair;

import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


@SuppressWarnings("deprecation")
public class OneMapper extends MapReduceBase implements Mapper<Text, Text, TextPair, TextPair> {
	
//	@Override
//	public void configure(JobConf job) {
//		// TODO Auto-generated method stub
//		super.configure(job);
//		Iterator<Entry<String,String>> conf = job.iterator();
//		while(conf.hasNext())
//		{
//			System.out.println(conf.next().toString());
//		}
//	}

	@Override
	public void map(Text key, Text values,
			OutputCollector<TextPair, TextPair> output, Reporter reporter) throws IOException {
		output.collect(new TextPair(key.toString(), "1"), new TextPair(values.toString(), "1"));
	}

}
