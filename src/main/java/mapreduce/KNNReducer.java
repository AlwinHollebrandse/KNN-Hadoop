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

        for (DoubleString val : values)
        {
            String type = val.getType();
            double tDist = val.getDistance();
            
            while (KnnMap.containsKey(tDist)) { // NOTE needed to handle duplicate distances as the tree wouldnt add them
                tDist += 0.000000001;
            }
            KnnMap.put(tDist, type);
            if (KnnMap.size() > k)
            {
                KnnMap.remove(KnnMap.lastKey());
            }
        }
        
        listOfKnnMaps.set(Integer.parseInt(key.toString()), KnnMap);
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (int test = 0; test < testInstancesLength; test++) {				
            TreeMap<Double, String> KnnMap = listOfKnnMaps.get(test);

            List<String> knnList = new ArrayList<String>(KnnMap.values());

            Map<String, Integer> freqMap = new HashMap<String, Integer>();
            
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
            context.write(testKey, new Text(mostCommonType)); // TODO add real class as well?
        }
    }
}