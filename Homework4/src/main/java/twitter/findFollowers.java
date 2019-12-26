package twitter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.ObjectUtils.Null;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class findFollowers extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(findFollowers.class);

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private final Text word = new Text();
		
		//splitting the input line by coma and getting the second value explicitly to get the followers
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String followers[]  = value.toString().split(",");
			
			if (followers[1] != "") {
				
				word.set(followers[1]);
				context.write(word, one);
			}
		}
	}

	static enum maxMin {maxVal, minVal, itr};
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private final IntWritable result = new IntWritable();
		int max = 0;
	
		
		public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
			int sum = 0;
			Counter min = context.getCounter(findFollowers.maxMin.minVal);
			Counter max = context.getCounter(findFollowers.maxMin.maxVal);
			
			for (final IntWritable val : values) {
				sum += val.get();
			}
			
			if(sum < min.getValue() || min.getValue() == 0) {
				
				min.setValue(sum);
			}
			if(sum > max.getValue()) {
				
				max.setValue(0);
				max.increment(sum);
			}
			result.set(sum);
			context.write(key, result);

		}
		
	}



	public static class TokenizerMapper2 extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private final Text word = new Text();
		
		static String centroids[] = new String[100];
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			
			String centroidPath = context.getConfiguration().get("centroids");
			BufferedReader br = null;
			FileReader fr = null;

			try {

				//br = new BufferedReader(new FileReader(FILENAME));
				fr = new FileReader(centroidPath+"/part-r-00000");
				br = new BufferedReader(fr);

				String sCurrentLine;
				int i = 0;
				while ((sCurrentLine = br.readLine()) != null) {
					centroids[i] = sCurrentLine;
					System.out.println("Cemtroid is "+i +" "+sCurrentLine);
					i++;
				}

			} catch (IOException e) {

				e.printStackTrace();

			} finally {

				try {

					if (br != null)
						br.close();

					if (fr != null)
						fr.close();

				} catch (IOException ex) {

					ex.printStackTrace();

				}

			}
			

		}
		//splitting the input line by coma and getting the second value explicitly to get the followers
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			IntWritable followerCount = new IntWritable(1);
			Text newCentroid = new Text();
			String newStrCentroid = "0";
			String followers[]  = value.toString().split(",");
			
			long min = Long.MAX_VALUE;
			
			if (followers[1] != "") {
			
			for (String val : centroids) {
				
				

				if(followers[1] != null && val != null) {
					//System.out.println("value is + "+val);
					String values[] = val.split(",");
				long diff = Math.abs(Long.parseLong(values[0]) - Long.parseLong(followers[1]));
				//System.out.println("diff is "+diff + " min is "+min);
				if(diff <= min) {
					min = diff;
					newStrCentroid = val;
					followerCount.set(Integer.parseInt(followers[1]));
				}
				}	
				
			}
			newCentroid.set(newStrCentroid);
		//	System.out.println("new centroid is "+newCentroid);
			context.write(newCentroid, followerCount);
			}
		}
		
		
		
	}
	
	public static class IntSumReducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {
		private final IntWritable result = new IntWritable();
		int max = 0;
	
		
		public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
			//Iterable<IntWritable> values2 = values;
			List<IntWritable> values2 = new ArrayList<>();
			long sum = 0;
			long avg = 0;
			int total = 0;
			for (final IntWritable val : values) {
				sum += val.get();
				values2.add(val);
				total++;
			}
		
			//System.out.println("Hiihdgvhjfv dghdghr grehgred sghreh "+total);
			avg = sum/total;
			Text newCentroid = new Text();
			String savg = avg+"";
			newCentroid.set(savg);
			
			Counter newCounter = context.getCounter(maxMin.itr);
			
			Counter sse = context.getCounter(findFollowers.SSE.valueOfSSE);
			double sseDouble = 0;
			for ( IntWritable val : values2) {
				sseDouble +=  Math.pow((val.get() - avg),2);
				//System.out.println("Hiihdgvhjfv dghdghr grehgred sghreh val "+val.get());
			}
			sseDouble += sse.getValue();
			sse.setValue(0);
			sse.setValue((long) sseDouble); 
			String keyArray[] = key.toString().split(",");
			//System.out.println("key value si "+key.toString());
			if(Integer.parseInt(keyArray[0].toString())!=avg) {
				newCounter.increment(1);
			}
			context.write(newCentroid, result);

		}
		
		
		
	}
	
	static enum SSE {valueOfSSE}
	@Override
	public int run(final String[] args) throws Exception {
	final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Word Count");
		job.setJarByClass(findFollowers.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");
		
		
		// Delete output directory, only to ease local development; will not work on AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
		// ================
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
         job.waitForCompletion(true);
		org.apache.hadoop.mapreduce.Counters allCounters = job.getCounters();
		
		
		long max = allCounters.findCounter(findFollowers.maxMin.maxVal).getValue();
		long min = allCounters.findCounter(findFollowers.maxMin.minVal).getValue();
		
		//long min = 1;
		//long max = 564512;
		//long interval = max/valueOfK;
		
		
		String centroidPath = args[2] + 0;
		int valueOfK = Integer.parseInt(args[3]);
		saveCentroids(min, max, centroidPath, valueOfK);
		
		
		
		long updateCount=1;
		int retVal = 0;
		int count =1;
		ArrayList<Long> allSSE = new ArrayList<Long>();
		
		while(updateCount > 0 && count <=10) {
			String inputPath = args[2] + (count-1);
			String outputPath = args[2] + count;
			
			final Configuration conf2 = getConf();
			final Job job2 = Job.getInstance(conf2, "Word Count");
			job2.setJarByClass(findFollowers.class);
			final Configuration jobConf2 = job2.getConfiguration();
			jobConf2.set("mapreduce.output.textoutputformat.separator", ",");
			
			FileInputFormat.addInputPath(job2, new Path(args[1]));
			FileOutputFormat.setOutputPath(job2, new Path(outputPath));
			job2.setMapperClass(TokenizerMapper2.class);
			job2.setReducerClass(IntSumReducer2.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(IntWritable.class);
			job2.getConfiguration().set("centroids", inputPath);
			 retVal =  job2.waitForCompletion(true) ? 0 : 1;
			 
			 org.apache.hadoop.mapreduce.Counters jc = job2.getCounters();
			 updateCount = jc.findCounter(findFollowers.maxMin.itr).getValue();
			 

			allSSE.add(jc.findCounter(findFollowers.SSE.valueOfSSE).getValue());
			 count++;
			 
		}
	
		for (int i = 0; i <allSSE.size(); i++) {
			
			System.out.println(allSSE.get(i));
		}
		
		
		return retVal;
		
	}
	
	public void saveCentroids(long min, long max, String path , int k) {
		
		long size = max - min;
		
		File path2 = new File(path);
		
		if(!path2.exists()) {
			
			path2.mkdirs();
		}
		
		String outFile = path2+"/part-r-00000";
		File outFilePath = new File(outFile);
		
		try {
			
			FileOutputStream fo = new FileOutputStream(outFilePath);
			if(!outFilePath.exists()) {
				outFilePath.createNewFile();
			}
			
			for (int i = 1; i <= k; i++) {
				
				StringBuilder build = new StringBuilder();
				int centroid = (int) Math.pow(10, i);
				//int centroid = (int) size/k;
				build.append(centroid + ",0");
				build.append("\n");
				fo.write(build.toString().getBytes());
			}
			
			fo.flush();
			fo.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	public static void main(final String[] args) {
		if (args.length != 4) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new findFollowers(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}