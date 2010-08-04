package joins.twoway;

import java.io.IOException;

import joins.twoway.TextPair;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


@SuppressWarnings("deprecation")
public class ZeroMapper extends MapReduceBase implements Mapper<Text, Text, TextPair, TextPair> {
	
	@Override
	public void map(Text key, Text values,
			OutputCollector<TextPair, TextPair> output, Reporter reporter) throws IOException {
		output.collect(new TextPair(key.toString(), "0"), new TextPair(values.toString(), "0"));
	}

}
