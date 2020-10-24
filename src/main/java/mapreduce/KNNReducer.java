package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KNNReducer extends Reducer<Text,DoubleString,Text,Text> {
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
            String type = val.getType();
            double tDist = val.getDistance();
            
            // Populate another TreeMap with the distance and model information extracted from the
            // DoubleString objects and trim it to size K as before.
            while (KnnMap.containsKey(tDist)) { // NOTE needed to handle duplicate distances as the tree wouldnt add them
                tDist += 0.000000001;
            }
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
        for (int test = 0; test < testInstancesLength; test++) {				
            TreeMap<Double, String> KnnMap = listOfKnnMaps.get(test);

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
                
            testKey.set("Test Index: " + Integer.toString(test) + ", predictedClass: ");
            context.write(testKey, new Text(mostCommonType)); // Use this line to produce a single classification // TODO add real class as well?
        }
    }
}