package mapreduce;

import org.apache.hadoop.io.TwoDArrayWritable;
import org.apache.hadoop.io.Writable;

public class DoubleIntegerTwoDArrayWritable extends TwoDArrayWritable implements Writable{
    public DoubleIntegerTwoDArrayWritable() { super(DoubleInteger.class); }

    public DoubleInteger[][] getAsDoubleInteger2DArray() {
        Writable[][] temp = super.get();
        if (temp != null) {
            int n = temp.length;
            int k = temp[0].length;
            DoubleInteger[][] items = new DoubleInteger[n][k];
            for (int i = 0; i < n; i++) {
                for (int j = 0; j < k; j++) {
                    items[i][j] = (DoubleInteger)temp[i][j];
                }
            }
            return items;
        } else {
            return null;
        }   
    }
}
