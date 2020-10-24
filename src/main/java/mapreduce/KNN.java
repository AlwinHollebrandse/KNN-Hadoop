package mapreduce;
//package Hadoop.MapReduce;
// PATH=$PATH:$HOME/hadoop-3.3.0/bin

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;  // Import the File class
import java.io.FileNotFoundException;  // Import this class to handle errors
import java.util.Scanner; // Import the Scanner class to read text files

public class KNN {
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

	    conf.set("k", args[3]);
		Job job = Job.getInstance(conf, "testKey count");
		job.setJarByClass(KNN.class);

		job.setMapperClass(KNNMapper.class);
		job.setReducerClass(KNNReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleString.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

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