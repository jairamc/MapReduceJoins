package joins.twoway;

import java.io.*;
import java.util.HashMap;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@SuppressWarnings("deprecation")
public class BroadCastJoin extends Configured implements Tool {

	public static class BroadCastJoinMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
	{
		File T1 = null;
		
		HashMap<String, ArrayList<String>> ht = null;
		
		@Override
		public void configure(JobConf conf) {
			//Read the broadcasted file
			T1 = new File(conf.get("broadcast.file"));
			ht = new HashMap<String, ArrayList<String>>();
			BufferedReader br = null;
			String line = null;
		    try{
		    	br = new BufferedReader(new FileReader(T1));
		    	while((line = br.readLine())!=null)
		    	{
		    		String record[] = line.split("\t", 2);
		    		if(record.length == 2)
		    		{
		    			//Insert into Hashtable
		    			if(ht.containsKey(record[0]))
		    			{
		    				ht.get(record[0]).add(record[1]);
		    			}
		    			else
		    			{
		    				ArrayList<String> value = new ArrayList<String>();
		    				value.add(record[1]);
		    				ht.put(record[0], value);
		    			}
		    		}
		    		
		    	}
		    }
		    catch(Exception e)
		    {
		    	e.printStackTrace();
		    }
		}
		
		@Override
		public void map(LongWritable lineNumber, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			
			//String[] leftRecord = null;
			String[] rightRecord = value.toString().split("\t",2);
			//Text joinedRecord = null;
			
			if(rightRecord.length == 2)
			{
				for(String leftRecord : ht.get(rightRecord[0]))
				//while((leftRecord = ht.get(rightRecord[0].hashCode()))!=null)
				{
						output.collect(new Text(rightRecord[0]), new Text(leftRecord + "\t" + rightRecord[1]));
				}
			}
			
		}

	}
	

	@Override
	public int run(String[] arg0) throws Exception {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(BroadCastJoin.class);

		conf.set("broadcast.file", "input/table1");
		// TODO: specify output types
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(conf, new Path("input/table2"));
		FileOutputFormat.setOutputPath(conf, new Path("output"));
		
		// TODO: specify a mapper
		conf.setMapperClass(BroadCastJoinMapper.class);

		conf.setNumReduceTasks(0);

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
	

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new BroadCastJoin(), args);
	    System.exit(exitCode);
	}

}
