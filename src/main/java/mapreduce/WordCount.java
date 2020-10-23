package mapreduce;
//package Hadoop.MapReduce;
// PATH=$PATH:$HOME/hadoop-3.3.0/bin

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;  // Import the File class
import java.io.FileNotFoundException;  // Import this class to handle errors
import java.util.Scanner; // Import the Scanner class to read text files

public class WordCount {
	
	public static class DoubleArrayWritable extends ArrayWritable {

	    public DoubleArrayWritable(DoubleWritable[] values) {
	        super(DoubleWritable.class, values);
	    }

	    @Override
	    public DoubleWritable[] get() {
	        return (DoubleWritable[]) super.get();
	    }

	    @Override
	    public String toString() {
	        DoubleWritable[] values = get();
	        return values[0].toString() + ", " + values[1].toString();
	    }
	}

	public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleArrayWritable>{ // TODO rename
		private final static DoubleArrayWritable instanceClass = new DoubleArrayWritable();
		private final static DoubleArrayWritable instanceDistance = new DoubleArrayWritable();

		private List<String> allTestInstances = new LinkedList<String>();
		private Text word = new Text();
		
		protected void setup(Context context) throws IOException, InterruptedException {
	        Configuration conf = context.getConfiguration();
	        for(String testInstance : conf.get("testInstances").split("\n")) {
	        	allTestInstances.add(testInstance);
	        }
	    }

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//			List<String> allTestInstances = Arrays.asList(value.toString().split("\n"));
			List<String> allTrainInstances = Arrays.asList(value.toString().split("\n"));
			
			for (int test = 0; test < allTestInstances.size(); test++) { // TODO go through train?
				
				List<String> testInstance = Arrays.asList(allTestInstances.get(test).split(","));
				
				for (int train = 0; test < allTrainInstances.size(); train++) {

					List<String> trainInstance = Arrays.asList(allTrainInstances.get(train).split(","));

			        // calc distance
					double distance = 0;
			        for (int i = 0; i < trainInstance.size() - 1; i++) {
						double diff = Double.parseDouble(testInstance.get(i)) - Double.parseDouble(trainInstance.get(i));
						distance += diff * diff;
					}
			        
			        instanceClass.set(Double.parseDouble(trainInstance.get(trainInstance.size() - 1)));
			        instanceDistance.set(Math.sqrt(distance));
			        
			        DoubleWritable[] temp = new DoubleWritable[2];
			        DoubleArrayWritable output = new DoubleArrayWritable(temp);

			        temp[0] = instanceClass;
			        temp[1] = instanceDistance;
			        output.set(temp);
			        
			        word.set(Integer.toString(test)); // "temp" // TODO all share same key? or is each key the test index
					context.write(word, new DoubleArrayWritable(output.get()));	
				}
//				word.set("temp"); // TODO all share same key?
//				context.write(word, new DoubleArrayWritable(output.get()));	
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> { // TODO rename
		private DoubleWritable result = new DoubleWritable();
		private double[][] classDistanceMatrix;
		private int testInstancesLength;

		protected void setup(Context context) throws IOException, InterruptedException {
	        Configuration conf = context.getConfiguration();
	        testInstancesLength = Integer.parseInt(conf.get("testInstancesLength"));
	        int k = Integer.parseInt(conf.get("k"));
	        classDistanceMatrix = new double[testInstancesLength][k * 2];
	    }
		
		public void reduce(Text key, Iterable<DoubleArrayWritable> values, Context context) throws IOException, InterruptedException {
		// public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			for (int test = 0; test < testInstancesLength; test++) { // TODO go through train?
				// TODO rn im assuming that map has output sorted by distance and not class or something
				// TODO check if sorted
				
				// add to list
				List<String> allClassesAndDistances = new LinkedList<String>();

				for (DoubleArrayWritable val : values) {
					allClassesAndDistances.add(val.get());
					// sum += val.get();
				}

				for (int i = 0; i < classDistanceMatrix[test].length; i++) {

				}

				// sort list

				//
			}
			
			int sum = 0;
			for (DoubleWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			// context.write(key, result);
		}
		
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// Performs majority voting using the first k first elements of an array
//			int kVoting(int k, float** shortestKDistances) {
//			    map<float, int> classCounter;
//			    for (int i = 0; i < k; i++) {
//			        classCounter[shortestKDistances[i][1]]++;
//			    }
//
//			    int voteResult = -1;
//			    int numberOfVotes = -1;
//			    for (auto i : classCounter) {
//			        if (i.second > numberOfVotes) {
//			            numberOfVotes = i.second;
//			            voteResult = i.first;
//			        }
//			    }
//			    return voteResult;
//			}
//		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 4) { // TODO add check that instances of train and test are same length
	      System.out.printf("Usage: KNN <input dir that holds train data> <test data file> <output dir> <k>\n");
	      System.exit(-1);
	    }
		
		Configuration conf = new Configuration();
		String testInstacesString = getTestInstances(args[1]);
	    conf.set("testInstances", testInstacesString);
        String[] allTestInstancesTemp = testInstacesString.split("\n");
        conf.set("testInstancesLength",  Integer.toString(allTestInstancesTemp.length));
	    conf.set("k", args[3]);
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);

		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class); // TODO whats this do?
		job.setReducerClass(IntSumReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleArrayWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleArrayWritable.class);// TODO correct? should it be a single class? output f cleanup or reducer. OUTPUT of ceanup, reducer doesnt have to write

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static String getTestInstances(String testFile) throws FileNotFoundException {
		File myObj = new File(testFile);
		Scanner myReader = new Scanner(myObj);
		StringBuilder result = new StringBuilder();
		while (myReader.hasNextLine()) {
			result.append(myReader.nextLine());
		}
		myReader.close();
		return result.toString();
	}
	
	// https://stackoverflow.com/questions/25432598/what-is-the-mapper-of-reducer-setup-used-for/25450627
	// https://stackoverflow.com/questions/28914596/mapreduce-output-arraywritable
}