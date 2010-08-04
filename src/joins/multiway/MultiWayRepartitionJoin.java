package joins.multiway;

import java.io.File;

import joins.twoway.TextPair;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@SuppressWarnings("deprecation")
public class MultiWayRepartitionJoin extends Configured implements Tool{
	
	public static class KeyPartitioner implements Partitioner<TextPair, TextPair> {
	    @Override
	    public int getPartition(TextPair key, TextPair value, int numPartitions) {
	      return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
	    }

		@Override
		public void configure(JobConf conf) {}
	  }

	/**
	 * @param args
	 */

	public static boolean deleteDir(File dir) {
	    if (dir.isDirectory()) {
	        String[] children = dir.list();
	        for (int i=0; i<children.length; i++) {
	            boolean success = deleteDir(new File(dir, children[i]));
	            if (!success) {
	                return false;
	            }
	        }
	    }
	    // The directory is now empty so delete it
	    return dir.delete();
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		JobConf conf = new JobConf(getConf(), getClass());
		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapOutputKeyClass(TextPair.class);
		conf.setMapOutputValueClass(TextPair.class);
		
		conf.setJarByClass(MultiWayRepartitionJoin.class);
		conf.setReducerClass(RepartitionReducer.class);
		conf.setMapperClass(RepartitionMapper.class);
		conf.setPartitionerClass(KeyPartitioner.class);
		conf.setOutputValueGroupingComparator(TextPair.FirstComparator.class);
		
		Path[] inputPaths = new Path[args.length-1];
		StringBuilder sb = new StringBuilder();
		for(int i=0;i<args.length-1;i++)
		{
			sb.append(args[i]+";"+i+";");
			inputPaths[i]=new Path(args[i]);
		}
		
		conf.set("tables.tags", sb.toString());
		
		
		FileInputFormat.setInputPaths(conf, inputPaths);
		
		FileOutputFormat.setOutputPath(conf, new Path(args[args.length-1]));
		
		JobClient.runJob(conf);
	    return 0;
	}
	
	public static void main(String[] args) throws Exception {
		 int exitCode = ToolRunner.run(new MultiWayRepartitionJoin(), args);
		    System.exit(exitCode);
	}
}
