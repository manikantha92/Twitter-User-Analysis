package com.asg3.shortestPath;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
/**
 * Hello world!
 *
 */
public class App extends Configured implements Tool
{
	private static final Logger logger = LogManager.getLogger(App.class);
	public static class EdgesMapper extends Mapper<Object, Text, Text, Text> {
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
			String allEdges[] = value.toString().split(",");
			Text sourceVertexNode = new Text (allEdges[0]);
			Text destinationNode = new Text (allEdges[1]);
			context.write(sourceVertexNode, destinationNode);
		}
	}
	
	public static class AdjListReducer extends Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text vertex, Iterable<Text> destVertices, Context context) throws IOException, InterruptedException {
			
			StringBuffer adjList = new StringBuffer();
			Text output = new Text();
			for (Text destVertex : destVertices) {
				
				adjList.append(destVertex);
				adjList.append(",");
			}
			
			String outputString = adjList.length() > 0 ? adjList.substring(0, adjList.length() - 1) : "";
			output.set(outputString);
			context.write(vertex, output);
		}
		
	}
	
	// Second mapper to find the shortest path fromVertex the sourceVertex specified in the conf
	public static class SPathMapper extends Mapper<Object, Text, Text, Text>{

 
    private static final String EMPTY_NODE = "null";

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
        Configuration conf = context.getConfiguration();
        
     // get the sourceVertex vertex fromVertex the conf
        String sourceVertex = conf.get("sourceVertex"); 
        String line = value.toString();
        
     // split the input on space to seperate node and its adjacency list
        String[] vertices = line.split("\\s+"); 
        
        // for vertices that have an empty adjacency list
        if (vertices.length < 2) {
            return;
        }
        
        String fromVertex = vertices[0];
        
        //pathe traversed till now
        String pathTraversed = sourceVertex;
        long distance = Long.MAX_VALUE;
        String previous = EMPTY_NODE;
        String zero = Long.toString(0);
        String inf = Long.toString(Long.MAX_VALUE);
        Text outKey = new Text(fromVertex);
        Text outValue = new Text();
        
        if (vertices.length == 2) {
            // for the First time, input is the adjacency list
        	// and if adjacency list contains source, distance is countUpdates as zero
            if (fromVertex.equals(sourceVertex)) {
                distance = 0;
                outValue.set(String.join(" ", vertices[1], zero, fromVertex));
            } else {
                outValue.set(String.join(" ", vertices[1], inf, EMPTY_NODE));
            }
        } else {
            // and for all other iterations after the first , input is the reducer's output
            distance = Long.parseLong(vertices[2]);
            pathTraversed = vertices[3];
            
            // set the traversed path for the vertex in value
            outValue.set(String.join(" ", vertices[1], vertices[2], vertices[3]));
        }
        logger.debug(outKey + " " + outValue);        
        context.write(outKey, outValue);
            
        for (String adjacentVertice : vertices[1].split(",")) {
            String startVertex = sourceVertex;
            if (adjacentVertice.equals(EMPTY_NODE)) {
                return;
            }
            outKey.set(adjacentVertice);
            long newDistance = distance;
            if (newDistance != Long.MAX_VALUE) {
            	
            	// updating the distance by one, as weight of all the path is 1
                newDistance += 1;
                if (!pathTraversed.equals(fromVertex)) {
                    startVertex = fromVertex + "," + pathTraversed;
                }
            
                outValue.set(String.join(" ", Long.toString(newDistance),
                        startVertex));
                logger.debug(outKey + " " + outValue);
                context.write(outKey, outValue);
            }
        }
    }
}
	
	
	public static class SPathReducer extends Reducer<Text, Text, Text, Text> {
    
    private Logger logger = Logger.getLogger(this.getClass());

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

        Text result = new Text();
        
        //initialvalue for minimum distance set to maximum
        long minimumDistance = Long.MAX_VALUE;
        String startVertex = null;
        String adjacentVertices = null;
        Counter countUpdates = context.getCounter(App.NextIteration.updateDistance);
      //initialvalue for destination set to maximum
        long existingDistance = Long.MAX_VALUE;

        // update distances of the adjacency list
        for (Text val : values) {
            String[] presentVertexAdjList = val.toString().split("\\s+");
            if (presentVertexAdjList.length == 2) {
                long distance = Long.parseLong(presentVertexAdjList[0]);
                if (distance < minimumDistance) {
                    minimumDistance = distance;
                    startVertex = presentVertexAdjList[1];
                }
            } else {
                existingDistance = Long.parseLong(presentVertexAdjList[1]);
                if (existingDistance < minimumDistance) {
                    minimumDistance = existingDistance;
                    startVertex = presentVertexAdjList[2];
                }
                adjacentVertices = presentVertexAdjList[0];
            }
        }

        if (minimumDistance < existingDistance) {
            countUpdates.increment(1);
        }
        
        result.set(String.join(" ", adjacentVertices,
                Long.toString(minimumDistance),
                startVertex));
        logger.debug(key + " " + result);
        context.write(key, result);
    }
}	
	static enum NextIteration {
        updateDistance
    }
	
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		final Configuration conf = getConf();
		

		final Job job = Job.getInstance(conf, "shortest path");
		job.setJarByClass(App.class);
		job.getConfiguration().set("join.type", "inner");
		job.getConfiguration().set("mapreduce.output.textoutputformat.seperator", " ");
		
		job.setMapperClass(EdgesMapper.class);
		job.setReducerClass(AdjListReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		
		
		
		// setting the source vertex in conf 
		conf.set("sourceVertex", args[2]);
        long updateDistance = 1;
        int code = 0;
        int numIterations = 1;
        
        // get an instance of the file system
        FileSystem tempFile = FileSystem.get(conf);

        while (updateDistance > 0) {
           
            String input, output;
            final Job job1 = Job.getInstance(conf, "shortetst path");
            if (numIterations == 1) {
                input = args[1];
            } else {
                input = args[1] + "-" + (numIterations - 1);
            }
            output = args[1] + "-" + numIterations;

            job1.setJarByClass(App.class);
            job1.getConfiguration().set("mapreduce.output.textoutputformat.seperator", " ");
            job1.setMapperClass(SPathMapper.class);
            job1.setReducerClass(SPathReducer.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job1, new Path(input));
            FileOutputFormat.setOutputPath(job1, new Path(output));
            code = job1.waitForCompletion(true) ? 0 : 1;

            // assigning a global counter to loop through the adjList
            Counters jobCounters = job1.getCounters();
            updateDistance = jobCounters.
                findCounter(NextIteration.updateDistance).getValue();
            if (numIterations > 1) {
                tempFile.delete(new Path(input), true);
               
            }
            numIterations += 1;
        }
        return code;
	}
	public static void main(final String[] args) {
		BasicConfigurator.configure();
		if (args.length != 3) {
			throw new Error("Three arguments required:\n<input-dir> <output-dir> source vertex");
		}

		try {
			ToolRunner.run(new App(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
	
}
