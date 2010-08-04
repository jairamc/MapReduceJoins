package joins.multiway;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;

@SuppressWarnings("deprecation")
public class PrePartitioner extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		//JobClient client = new JobClient();
		JobConf conf = new JobConf(PrePartitioner.class);
		
		conf.setInputFormat(KeyValueTextInputFormat.class);
		//conf.setOutputFormat(TextOutputFormat.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(IdentityMapper.class);
		conf.setReducerClass(IdentityReducer.class);

		FileInputFormat.setInputPaths(conf, args[args.length - 2]);		
		FileOutputFormat.setOutputPath(conf, new Path(args[args.length - 1]));

		JobClient.runJob(conf);
		
		return 0;
	}

}
