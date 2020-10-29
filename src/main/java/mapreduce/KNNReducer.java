package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class KNNReducer extends Reducer<IntWritable,DoubleIntegerTwoDArrayWritable,IntWritable,IntWritable> {
    private IntWritable actualType = new IntWritable();
    private IntWritable predictedType = new IntWritable();
    private List<String> allTestInstances = new LinkedList<String>();
    private int testInstancesLength;
    private int k;
    private List<TreeMap<Double, Integer>> listOfKnnMaps = new ArrayList<TreeMap<Double, Integer>>();


    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        testInstancesLength = Integer.parseInt(conf.get("testInstancesLength"));
        for(String testInstance : conf.get("testInstances").split("\n")) {
            allTestInstances.add(testInstance);
        }
        k = Integer.parseInt(conf.get("k"));
        
        for (int i = 0; i < testInstancesLength; i++) {
            listOfKnnMaps.add(new TreeMap<Double, Integer>());
        }
    }
    
    public void reduce(IntWritable key, Iterable<DoubleIntegerTwoDArrayWritable> values, Context context) throws IOException, InterruptedException {
        for (DoubleIntegerTwoDArrayWritable val : values) {
            for (int test = 0; test < allTestInstances.size(); test++) {
                TreeMap<Double, Integer> KnnMap = listOfKnnMaps.get(test);
                for (DoubleInteger pair : val.getAsDoubleInteger2DArray()[test])
                {
                    Integer type = pair.getType();
                    double tDist = pair.getDistance();
                    
                    while (KnnMap.containsKey(tDist) && tDist != Double.MAX_VALUE) { // NOTE needed to handle duplicate distances as the tree wouldnt add them
                        tDist += 0.000000001;
                    }
                    KnnMap.put(tDist, type);
                    if (KnnMap.size() > k)
                    {
                        KnnMap.remove(KnnMap.lastKey());
                    }
                }
                listOfKnnMaps.set(test, KnnMap);        
            }
        }
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (int test = 0; test < testInstancesLength; test++) {				
            TreeMap<Double, Integer> KnnMap = listOfKnnMaps.get(test);

            List<Integer> knnList = new ArrayList<Integer>(KnnMap.values());

            Map<Integer, Integer> freqMap = new HashMap<Integer, Integer>();
            
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
            
            Integer mostCommonType = null;
            int maxFrequency = -1;
            for(Map.Entry<Integer, Integer> entry: freqMap.entrySet())
            {
                if(entry.getValue() > maxFrequency)
                {
                    mostCommonType = entry.getKey();
                    maxFrequency = entry.getValue();
                }
            }
                
            List<String> testInstance = Arrays.asList(allTestInstances.get(test).split(","));
            actualType.set(Integer.parseInt(testInstance.get(testInstance.size() - 1)));
            predictedType.set(mostCommonType);
            context.write(actualType, predictedType);
        }
    }
}