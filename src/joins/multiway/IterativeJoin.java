package joins.multiway;

import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;

import joins.twoway.RepartitionJoin;

@SuppressWarnings("deprecation")
public class IterativeJoin extends Configured {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		String input1 = args[args.length - 4];
		String input2 = args[args.length - 3];
		String input3 = args[args.length - 2];
		String output = args[args.length - 1];
		
		//Preprocessing
		
		//Find output cardinalities
		String[] preProc = new String[args.length+1];
		preProc[0] = "PreProc";
		for(int i=0;i<args.length;i++)
		{
			preProc[i+1] = args[i];
		}
		
		CardinalityCounter cc = new CardinalityCounter();
		ToolRunner.run(cc, preProc);
			
		RunningJob job = cc.job;
		if(job == null)
		{
			System.err.println("No Jobs found");
		}
		while(!job.isComplete());
		
		Iterator<Counters.Counter> cardinalities = job.getCounters().getGroup("OutputCardinality").iterator();
		
		long c12=0,c23=0,c13=0;
		while(cardinalities.hasNext())
		{
			Counters.Counter cardinality = cardinalities.next(); 
			if(cardinality.getName().contains(input1) && cardinality.getName().contains(input2))
			{
				c12 = cardinality.getValue();
			}
			else if(cardinality.getName().contains(input2) && cardinality.getName().contains(input3))
			{
				c23 = cardinality.getValue();
			}
			else
			{
				c13 = cardinality.getValue();
			}
			System.out.println(cardinality.getName()+":"+cardinality.getValue());
		}
		
		
//		long c12 = job.getCounters().findCounter("OutputCardinality", input1+"+"+input2).getValue();
//		long c13 = job.getCounters().findCounter("OutputCardinality", input1+"+"+input3).getValue();
//		long c23 = job.getCounters().findCounter("OutputCardinality", input2+"+"+input3).getValue();
		
		//End Preprocessing
		
		
		String intermediatePath = output+"/intermediate";
		String job1[] = new String[3];
		String job2[] = new String[3];
		if(c12<=c23 && c12<=c13)
		{
			job1[0] = input1; job1[1] = input2; job1[2] = intermediatePath;
			job2[0] = intermediatePath; job2[1] = input3; job2[2]= output+"/final";
		}
		else if(c23<=c13 && c23<=c12)
		{
			job1[0] = input2; job1[1] = input3; job1[2] = intermediatePath;
			job2[0] = intermediatePath; job2[1] = input1; job2[2]= output+"/final";
			
		}
		else
		{
			job1[0] = input1; job1[1] = input3; job1[2] = intermediatePath;
			job2[0] = intermediatePath; job2[1] = input2; job2[2]= output+"/final";
		}

		
		ToolRunner.run(new RepartitionJoin(), job1);
		ToolRunner.run(new RepartitionJoin(), job2);
	}

}
