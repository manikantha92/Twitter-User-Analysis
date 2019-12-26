package com.asg2.asg2RSJoin;

import java.io.Console;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class App extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(App.class);

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		private final static IntWritable one = new IntWritable(1);
		
		private final Text from = new Text();
		private final Text to = new Text();
		private final Text from1 = new Text();
		private final Text to1 = new Text();
		private final Text fromFlag = new Text("F");
		private final Text toFlag = new Text("T");
		
		//splitting the input line by coma and getting the second value explicitly to get the followers
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String followers[]  = value.toString().split(",");
			
			if (followers[1] != "" || followers[0] != "") {
				
				from.set(fromFlag + followers[0] + "," + followers[1]);
				
				String splitKey[] = value.toString().split(",");
				to.set(splitKey[0]);
				context.write(to,from);
				//System.out.println("to "+to.toString());
				//System.out.println("from "+from.toString());
				
				
				from1.set(toFlag + followers[1]+ "," + followers[0]);
				String splitKey1[] = value.toString().split(",");
				//System.out.println("split keys " + splitKey1[0] + " first value "+splitKey1[1]);
				to1.set(splitKey1[1]);
				context.write(to1,from1);
				//System.out.println("to1 "+to1.toString());
				//System.out.println("from1 "+from1.toString());
				
				
			}
		}
		
		
	}
	
	public static class TokenizerMapper2 extends Mapper<Object, Text, Text, Text> {
		private final static IntWritable one = new IntWritable(1);
		private final Text from = new Text();
		private final Text to = new Text();
		private final Text from1 = new Text();
		private final Text to1 = new Text();
		private final Text fromFlag = new Text("F");
		private final Text toFlag = new Text("T");
		
		//splitting the input line by coma and getting the second value explicitly to get the followers
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String followers[]  = value.toString().split(",");
			
			if (followers[1] != "" || followers[0] != "") {
				
				from.set(followers[1] + "," + followers[0]);
				to.set(toFlag);
				context.write(from,to);
				
			}
		}
		
		
	}
	
	public static class TokenizerMapper3 extends Mapper<Object, Text, Text, Text> {
		private final static IntWritable one = new IntWritable(1);
		private final Text from = new Text();
		private final Text to = new Text();
		private final Text from1 = new Text();
		private final Text to1 = new Text();
		private final Text fromFlag = new Text("F");
		private final Text toFlag = new Text("T");
		
		//splitting the input line by coma and getting the second value explicitly to get the followers
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String followers[]  = value.toString().split(",");
			
			if (followers[1] != "" || followers[0] != "") {
				
				//System.out.println("In mapper 3 " + followers[0] + " second part" + followers[1]);
				from.set(followers[0]+","+ followers[1].charAt(0));
				to.set("F");
				context.write(from, to);
				
				
				
			}
		}
		
		
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		private final Text result = new Text();
		private ArrayList<String> listA = new ArrayList<String>();
		private ArrayList<String> listB = new ArrayList<String>();
		private String joinType = null;

		@Override
		public void setup(Context context) {
			// Get the type of join from our configuration
			joinType = context.getConfiguration().get("join.type");
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// Clear our lists
			listA.clear();
			listB.clear();

			// iterate through all our values, binning each record based on what
			// it was tagged with
			// make sure to remove the tag!
			
			for (Text t : values) {
				//System.out.println("values "+t.toString());
				if (t.toString().charAt(0) == 'F') {
					listA.add(t.toString().substring(1));
				//System.out.println("list a is "+listA);
				} else if (t.toString().charAt(0) == 'T') {
					listB.add(t.toString().substring(1));
				//	System.out.println("list b is " +listB);
					
				}
			}
			executeJoinLogic(context);
		}
			// Execute our join logic now that the lists are filled
			private void executeJoinLogic(Context context) throws IOException,
			InterruptedException {
			if (joinType.equalsIgnoreCase("inner")) {
			if (!listA.isEmpty() && !listB.isEmpty()) {
				for (String F : listA) {
					for (String T : listB) {
						String fString = F.toString();
						String tString = T.toString();
						
						String[] fTokens = fString.split(",");
						//System.out.println("ftokens is " +fTokens[0] + " " + fTokens[1]);
						String[] tTokens = tString.split(",");
						//System.out.println("ttokens is " +tTokens[0] + " " + tTokens[1]);
						
						if(tTokens[0].equals(fTokens[0])) {
						Text F1 = new Text(tTokens[1] + "," + fTokens[1]);
						Text T1 = new Text("F");
									
						context.write(F1,T1);
						
						}
					
						
					}
				}
			}
			}
			}
	}
	
	public static class FilterMapper extends Mapper<Object, Text, Text, Text> {

		  protected void map(Object key, Text value, Context context)
		      throws java.io.IOException, InterruptedException {
		    String[] tokens = value.toString().split(",");
		    char snew =  tokens[1].charAt(0);
		   // System.out.println("char is "+ snew);
		    int f1 = Integer.parseInt(tokens[0]);
		    int f2 = Integer.parseInt(String.valueOf(snew));
		  //  System.out.println("int is "+ f2);
		    if(f1 <= 100 && f2 <= 100){
		    //	System.out.println("f1 " +f1);
		    //	System.out.println("f2 " +f2);
		    	Text f1Text = new Text();
		    	f1Text.set(Integer.toString(f1));
		    	
		    	Text f2Text = new Text();
		    	f2Text.set("," + Integer.toString(f2));
		    	
		    	context.write(f1Text, f2Text);
		    }
		  }  
	}
	
	public static class IntSumReducer1 extends Reducer<Text, Text, Text, Text> {
		private static int count = 0;
		private final Text result = new Text();
		private ArrayList<String> listA = new ArrayList<String>();
		private ArrayList<String> listB = new ArrayList<String>();
		private String joinType = null;

		@Override
		public void setup(Context context) {
			// Get the type of join from our configuration
			joinType = context.getConfiguration().get("join.type");
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// Clear our lists
			listA.clear();
			listB.clear();

			// iterate through all our values, binning each record based on what
			// it was tagged with
			// make sure to remove the tag!
			
			for (Text t : values) {
			
				if(t.toString().equals("F")) {
					count++;
					//System.out.println("value are " +t.toString());
					context.write(key, t);
				}
				
			}
			//System.out.println("count is "+ count);
			
	}
	}
	
	public int run(final String[] args) throws Exception {
/*		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Word Count");
		job.setJarByClass(App.class);
		job.getConfiguration().set("join.type", "inner");
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		
		
		// Delete output directory, only to ease local development; will not work on AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
		// ================
		
		MultipleInputs.addInputPath(job, new Path(args[0]),
				TextInputFormat.class, TokenizerMapper.class);

		MultipleInputs.addInputPath(job, new Path(args[2]),
				TextInputFormat.class, TokenizerMapper2.class);
		//job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));*/
		
		Configuration conf = new Configuration();
		Path out = new Path(args[1]);
		

		Job job1 = Job.getInstance(conf, "Word Count");
		job1.setJarByClass(App.class);
		job1.getConfiguration().set("join.type", "inner");
		job1.setMapperClass(TokenizerMapper.class);
		job1.setReducerClass(IntSumReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);
		
	    Job job = Job.getInstance(conf, "Filter");
	    job.setJarByClass(App.class);
	    job.setMapperClass(FilterMapper.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	   
	    
	    FileInputFormat.addInputPath(job, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    if (!job.waitForCompletion(true)) {
			  System.exit(1);
			}
	    
	    
	    
	    
		
		Job job2 = Job.getInstance(conf, "Word Count");
		job2.setJarByClass(App.class);
		job2.getConfiguration().set("join.type", "inner");
		job2.setReducerClass(IntSumReducer1.class);
		//job2.setNumReduceTasks(1);
		//job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		 MultipleInputs.addInputPath(job2, new Path(args[0]), TextInputFormat.class, TokenizerMapper2.class);
		 MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class, TokenizerMapper3.class);  
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		if (!job2.waitForCompletion(true)) {
		  System.exit(1);
		}
		return job2.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(final String[] args) {
		if (args.length != 4) {
			throw new Error("Three arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new App(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}