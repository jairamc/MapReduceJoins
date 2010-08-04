package joins.twoway;

import java.io.IOException; 
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

@SuppressWarnings("deprecation")
public class RepartitionReducer extends MapReduceBase implements Reducer<TextPair, TextPair, Text, Text> {

	@Override
	public void reduce(TextPair key, Iterator<TextPair> values, 
			OutputCollector<Text, Text> output,  Reporter reporter)
			throws IOException {
		ArrayList<String> T1 = new ArrayList<String>();
		String tag = key.getSecond().toString();
		TextPair value = null;
		
		while(values.hasNext())
		//for(TextPair value : values)
		{
			value = values.next();
			if(value.getSecond().toString().compareTo(tag)==0)
			{
				T1.add(value.getFirst().toString());
			}
			else
			{
				for(String val : T1)
				{
					output.collect(new Text(key.getFirst()), new Text(val + "\t" + value.getFirst().toString()));
				}
			}
		}
	}
}
