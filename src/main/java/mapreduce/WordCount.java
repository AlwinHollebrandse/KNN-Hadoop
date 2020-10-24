package mapreduce;
//package Hadoop.MapReduce;
// PATH=$PATH:$HOME/hadoop-3.3.0/bin

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;  // Import the File class
import java.io.FileNotFoundException;  // Import this class to handle errors
import java.util.Scanner; // Import the Scanner class to read text files

public class WordCount {

	// WritableComparable class for a paired Double and String (distance and type)
	// This is a custom class for MapReduce to pass a double and a String through context
	// as one serializable object.
	// This example only implements the minimum required methods to make this job run. To be
	// deployed robustly is should include ToString(), hashCode(), WritableComparable interface
	// if this object was intended to be used as a key etc.
	public static class DoubleString implements WritableComparable<DoubleString>
	{
		private Double distance = 0.0;
		private String type = null;

		public void set(Double lhs, String rhs)
		{
			distance = lhs;
			type = rhs;
		}
		
		public Double getDistance()
		{
			return distance;
		}
		
		public String getType()
		{
			return type;
		}
		
		@Override
		public void readFields(DataInput in) throws IOException
		{
			distance = in.readDouble();
			type = in.readUTF();
		}
		
		@Override
		public void write(DataOutput out) throws IOException
		{
			out.writeDouble(distance);
			out.writeUTF(type);
		}
		
		@Override
		public int compareTo(DoubleString o)
		{
			return (this.type).compareTo(o.type);
		}
	}

	public static class KNNMapper extends Mapper<Object, Text, Text, DoubleString>{
		private DoubleString distanceAndType = new DoubleString();
		private List<String> allTestInstances = new LinkedList<String>();
		private Text testKey = new Text();
		private int k;
		
		protected void setup(Context context) throws IOException, InterruptedException {
	        Configuration conf = context.getConfiguration();
	        for(String testInstance : conf.get("testInstances").split("\n")) {
	        	allTestInstances.add(testInstance);
			}
			System.out.println("allTestInstances.size(): " + allTestInstances.size());
			k = Integer.parseInt(conf.get("k"));
	    }

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//			List<String> allTestInstances = Arrays.asList(value.toString().split("\n"));
			System.out.println("value: " + value.toString());

			List<String> allTrainInstances = Arrays.asList(value.toString().split("\n"));
			System.out.println("allTrainInstances size: " + allTrainInstances.size());
			System.out.println("allTrainInstances: " + allTrainInstances.toString());

			for (int test = 0; test < allTestInstances.size(); test++) { // TODO go through train?
				TreeMap<Double, String> KnnMap = new TreeMap<Double, String>();
				
				List<String> testInstance = Arrays.asList(allTestInstances.get(test).split(","));
				
				for (int train = 0; train < allTrainInstances.size(); train++) {

					List<String> trainInstance = Arrays.asList(allTrainInstances.get(train).split(","));

			        // calc distance
					double distance = 0;
			        for (int i = 0; i < trainInstance.size() - 1; i++) {
						double diff = Double.parseDouble(testInstance.get(i)) - Double.parseDouble(trainInstance.get(i));
						distance += diff * diff;
					}

					// Add the total distance and corresponding car type for this row into the TreeMap with distance
					// as key and type as value.
					KnnMap.put(Math.sqrt(distance), trainInstance.get(trainInstance.size() - 1));
					// Only K distances are required, so if the TreeMap contains over K entries, remove the last one
					// which will be the highest distance number.
					if (KnnMap.size() > k)
					{
						KnnMap.remove(KnnMap.lastKey());
					}

				}
				System.out.println("map test: " + test + ", KnnMap: " + KnnMap.toString());

				// Loop through the K key:values in the TreeMap
				for(Map.Entry<Double, String> entry : KnnMap.entrySet())
				{
					Double knnDist = entry.getKey();
					String knntype = entry.getValue();
					// distanceAndType is the instance of DoubleString declared aerlier
					distanceAndType.set(knnDist, knntype);
					testKey.set(Integer.toString(test));
					// Write to context a NullWritable as key and distanceAndType as value
					context.write(testKey, distanceAndType); // TODO was NullWritable
				}
			}
		}
	}

	public static class KNNReducer extends Reducer<Text,DoubleString,Text,Text> {
		private Text testKey = new Text();
		private int testInstancesLength;
		private int k;
		private List<TreeMap<Double, String>> listOfKnnMaps = new ArrayList<TreeMap<Double, String>>();


		protected void setup(Context context) throws IOException, InterruptedException {
	        Configuration conf = context.getConfiguration();
	        testInstancesLength = Integer.parseInt(conf.get("testInstancesLength"));
	        k = Integer.parseInt(conf.get("k"));
			
			for (int i = 0; i < testInstancesLength; i++) {
				listOfKnnMaps.add(new TreeMap<Double, String>());
			  }
	    }
		
		public void reduce(Text key, Iterable<DoubleString> values, Context context) throws IOException, InterruptedException {

			TreeMap<Double, String> KnnMap = new TreeMap<Double, String>();

			// values are the K DoubleString objects which the mapper wrote to context
			// Loop through these
			for (DoubleString val : values)
			{
				// System.out.println();
				String type = val.getType();
				double tDist = val.getDistance();
				
				// Populate another TreeMap with the distance and model information extracted from the
				// DoubleString objects and trim it to size K as before.
				KnnMap.put(tDist, type);
				if (KnnMap.size() > k)
				{
					KnnMap.remove(KnnMap.lastKey());
				}
			}
			
			// save the KnnMap of that test instance to the correct index of the testInstance array
			listOfKnnMaps.set(Integer.parseInt(key.toString()), KnnMap);
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// This section determines which of the K values (models) in the TreeMap occurs most frequently
			// by means of constructing an intermediate ArrayList and HashMap.
			System.out.println("cleanup listOfKnnMaps size: " + listOfKnnMaps.size()); // should be size of test set
			for (int test = 0; test < testInstancesLength; test++) {				
				TreeMap<Double, String> KnnMap = listOfKnnMaps.get(test);
				System.out.println("reduce test: " + test + ", KnnMap: " + KnnMap.toString());

				// A List of all the values in the TreeMap.
				List<String> knnList = new ArrayList<String>(KnnMap.values());

				Map<String, Integer> freqMap = new HashMap<String, Integer>();
				
				// Add the members of the list to the HashMap as keys and the number of times each occurs
				// (frequency) as values
				for(int i=0; i< knnList.size(); i++)
				{  
					Integer frequency = freqMap.get(knnList.get(i));
					if(frequency == null)
					{
						freqMap.put(knnList.get(i), 1);
					} else
					{
						freqMap.put(knnList.get(i), frequency+1);
					}
				}
				
				// Examine the HashMap to determine which key (model) has the highest value (frequency)
				String mostCommonType = null;
				int maxFrequency = -1;
				for(Map.Entry<String, Integer> entry: freqMap.entrySet())
				{
					if(entry.getValue() > maxFrequency)
					{
						mostCommonType = entry.getKey();
						maxFrequency = entry.getValue();
					}
				}
					
				// Finally write to context another NullWritable as key and the most common model just counted as value. // TODO nullwritable comments
				testKey.set("Test Index: " + Integer.toString(test) + ", predictedClass: ");
				// Write to context a NullWritable as key and distanceAndType as value
				context.write(testKey, new Text(mostCommonType)); // Use this line to produce a single classification // TODO add real class as well?
	//			context.write(NullWritable.get(), new Text(KnnMap.toString()));	// Use this line to see all K nearest neighbours and distances
			}
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 4) { // TODO add check that instances of train and test are same length
	      System.out.printf("Usage: KNN <input dir that holds train data> <output dir> <test data file>  <k>\n");
	      System.exit(-1);
	    }
		
		Configuration conf = new Configuration();
		String testInstacesString = getTestInstances(args[2]);
	    conf.set("testInstances", testInstacesString);
        String[] allTestInstancesTemp = testInstacesString.split("\n");
		conf.set("testInstancesLength",  Integer.toString(allTestInstancesTemp.length));
		System.out.println("HERE: " + allTestInstancesTemp.length);
		// System.out.println("testInstacesString: \n" + testInstacesString);

	    conf.set("k", args[3]);
		Job job = Job.getInstance(conf, "testKey count");
		job.setJarByClass(WordCount.class);

		job.setMapperClass(KNNMapper.class);
		// job.setCombinerClass(KNNReducer.class); // TODO whats this do?
		job.setReducerClass(KNNReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleString.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);// TODO correct? should it be a single class? output f cleanup or reducer. OUTPUT of ceanup, reducer doesnt have to write

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static String getTestInstances(String testFile) throws FileNotFoundException {
		File myObj = new File(testFile);
		Scanner myReader = new Scanner(myObj);
		StringBuilder result = new StringBuilder();
		while (myReader.hasNextLine()) {
			result.append(myReader.nextLine());
			result.append("\n");
		}
		myReader.close();
		return result.toString();
	}
	
	// https://stackoverflow.com/questions/25432598/what-is-the-mapper-of-reducer-setup-used-for/25450627
	// https://stackoverflow.com/questions/28914596/mapreduce-output-arraywritable
//	https://github.com/matt-hicks/MapReduce-KNN/blob/master/KnnPattern.java
}