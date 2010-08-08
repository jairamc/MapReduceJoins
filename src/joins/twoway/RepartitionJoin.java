package joins.twoway;

import java.io.File;

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@SuppressWarnings("deprecation")
public class RepartitionJoin extends Configured implements Tool{
	
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
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapOutputKeyClass(TextPair.class);
		conf.setMapOutputValueClass(TextPair.class);
		
		conf.setJarByClass(RepartitionJoin.class);
		conf.setReducerClass(RepartitionReducer.class);
		
		conf.setPartitionerClass(KeyPartitioner.class);
		conf.setOutputValueGroupingComparator(TextPair.FirstComparator.class);
		
		
//		Path input1 = new Path(args[args.length - 3]);
//		Path input2 = new Path(args[args.length - 2]);
		Path output = new Path(args[args.length - 1]);
//
//		MultipleInputs.addInputPath(conf,  input1,
//				KeyValueTextInputFormat.class, ZeroMapper.class);
//		
//		MultipleInputs.addInputPath(conf,  input2,
//				KeyValueTextInputFormat.class, OneMapper.class);
		
		conf.setMapperClass(ZeroMapper.class);
		conf.setInputFormat(KeyValueTextInputFormat.class);
	
		Path[] inputPaths = new Path[args.length-1];
		StringBuilder sb = new StringBuilder();
		for(int i=0;i<args.length-1;i++)
		{
			sb.append(args[i]+";"+i+";");
			inputPaths[i]=new Path(args[i]);
		}
		
		conf.set("tables.tags", sb.toString());
		
		
		FileInputFormat.setInputPaths(conf, inputPaths);
		
		FileOutputFormat.setOutputPath(conf, output);
		
		JobClient jc = new JobClient(conf);
		
		ClusterStatus cluster = jc.getClusterStatus();
		conf.setNumReduceTasks(cluster.getMaxReduceTasks()-2);
		
		JobClient.runJob(conf);
		//jc.run(arg0)
	    return 0;
	}
	
	public static void main(String[] args) throws Exception {
		 int exitCode = ToolRunner.run(new RepartitionJoin(), args);
		 System.out.println(exitCode);
		    System.exit(exitCode);
	}
}
