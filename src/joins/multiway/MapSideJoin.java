package joins.multiway;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.join.CompositeInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

@SuppressWarnings("deprecation")
public class MapSideJoin {

	public static void main(String[] args) throws Exception {
		
		for(int i=0;i<args.length-1;i++) //the last args is not included. Thats the output path
		{
			ToolRunner.run(new PrePartitioner(), new String[]{args[i], "partitioned/"+args[i]});
		}
		
		JobConf conf = new JobConf();
		conf.setJarByClass(MapSideJoin.class);
		conf.setJobName("MapSideJoin");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		//Main Map side join 
		conf.setNumReduceTasks(0);
		FileOutputFormat.setOutputPath(conf, new Path(args[args.length - 1]));
		conf.setInputFormat(CompositeInputFormat.class);
		
	    List<Path> plist = new ArrayList<Path>(args.length);
	    for (int i=0;i<args.length-1;i++) {
	      plist.add(new Path("partitioned/"+args[i]));
	    }

	    conf.set("mapred.join.expr", 
				CompositeInputFormat.compose("inner", KeyValueTextInputFormat.class, 
						plist.toArray(new Path[0])));
		
		JobClient.runJob(conf);
		
	}


}
