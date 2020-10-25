package mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class KNNMapper extends Mapper<Object, Text, IntWritable, DoubleInteger>{
    private DoubleInteger distanceAndType = new DoubleInteger();
    private List<String> allTestInstances = new LinkedList<String>();
    private IntWritable testKey = new IntWritable();
    private int k;
    
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        for(String testInstance : conf.get("testInstances").split("\n")) {
            allTestInstances.add(testInstance);
        }
        k = Integer.parseInt(conf.get("k"));
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        List<String> allTrainInstances = Arrays.asList(value.toString().split("\n"));
        // System.out.println("allTrainInstances size: " + allTrainInstances.size());

        for (int test = 0; test < allTestInstances.size(); test++) {
            TreeMap<Double, Integer> KnnMap = new TreeMap<Double, Integer>();
            
            List<String> testInstance = Arrays.asList(allTestInstances.get(test).split(","));
            
            for (int train = 0; train < allTrainInstances.size(); train++) {

                List<String> trainInstance = Arrays.asList(allTrainInstances.get(train).split(","));

                // calc distance
                double distance = 0;
                for (int i = 0; i < trainInstance.size() - 1; i++) {
                    double diff = Double.parseDouble(testInstance.get(i)) - Double.parseDouble(trainInstance.get(i));
                    distance += diff * diff;
                }

                distance = Math.sqrt(distance);
                Integer type = Integer.parseInt(trainInstance.get(trainInstance.size() - 1));
                while (KnnMap.containsKey(distance)) { // NOTE needed to handle duplicate distances as the tree wouldnt add them
                    distance += 0.000000001;
                }
                KnnMap.put(distance, type);

                if (KnnMap.size() > k)
                {
                    KnnMap.remove(KnnMap.lastKey());
                }

            }

            for(Map.Entry<Double, Integer> entry : KnnMap.entrySet())
            {
                Double knnDist = entry.getKey();
                Integer knntype = entry.getValue();
                distanceAndType.set(knnDist, knntype);
                testKey.set(test);
                context.write(testKey, distanceAndType);
            }
        }
    }
}