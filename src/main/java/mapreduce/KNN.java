package mapreduce;
// PATH=$PATH:$HOME/hadoop-3.3.0/bin

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class KNN {
	public static void main(String[] args) throws Exception {
		if (args.length != 4) { // TODO add check that instances of train and test are same length for each data point
	      System.out.printf("Usage: KNN <input dir that holds train data> <output dir> <test data file>  <k>\n");
	      System.exit(-1);
	    }
		
		Configuration conf = new Configuration();
		String testInstacesString = getTestInstances(args[2]);
	    conf.set("testInstances", testInstacesString);
        String[] allTestInstancesTemp = testInstacesString.split("\n");
		conf.set("testInstancesLength",  Integer.toString(allTestInstancesTemp.length));

	    conf.set("k", args[3]);
		Job job = Job.getInstance(conf, "testKey count");
		// job.getConfiguration().setInt(LINES_PER_MAP, 300); // TODO doesnt work
		job.setJarByClass(KNN.class);

		job.setMapperClass(KNNMapper.class);
		job.setReducerClass(KNNReducer.class);

		job.setMapOutputKeyClass(IntWritable.class); // TODO int writable?
		job.setMapOutputValueClass(DoubleInteger.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		// TODO add accuracy computing
		// TODO add timing
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
}