package mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KNNMapper extends Mapper<Object, Text, Text, DoubleString>{
    private DoubleString distanceAndType = new DoubleString();
    private List<String> allTestInstances = new LinkedList<String>();
    private Text testKey = new Text();
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
                distance = Math.sqrt(distance);
                String type = trainInstance.get(trainInstance.size() - 1);
                while (KnnMap.containsKey(distance)) { // NOTE needed to handle duplicate distances as the tree wouldnt add them
                    distance += 0.00000000000001;
                }
                KnnMap.put(distance, type);
                // Only K distances are required, so if the TreeMap contains over K entries, remove the last one
                // which will be the highest distance number.
                if (KnnMap.size() > k)
                {
                    KnnMap.remove(KnnMap.lastKey());
                }

            }

            // Loop through the K key:values in the TreeMap
            for(Map.Entry<Double, String> entry : KnnMap.entrySet())
            {
                Double knnDist = entry.getKey();
                String knntype = entry.getValue();
                distanceAndType.set(knnDist, knntype);
                testKey.set(Integer.toString(test));
                context.write(testKey, distanceAndType);
            }
        }
    }
}