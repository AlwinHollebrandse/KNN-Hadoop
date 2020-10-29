package mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
// import org.apache.hadoop.io.TwoDArrayWritable;

// public class DoubleIntegerTwoDArrayWritable implements TwoDArrayWritable {
//     public DoubleIntegerTwoDArrayWritable() { super(DoubleInteger.class); }
// }


public class DoubleIntegerArrayWritable extends ArrayWritable implements Writable {
    public DoubleIntegerArrayWritable() { super(DoubleInteger.class); }
}

public class DoubleIntegerArrayArrayWritable extends ArrayWritable implements Writable {
    public DoubleIntegerArrayArrayWritable() { super(DoubleIntegerArrayWritable.class); }
}


// WritableComparable class for a paired Double and Integer (distance and type)
// This is a custom class for MapReduce to pass a double and an Integer through context
// as one serializable object.
public class DoubleInteger implements WritableComparable<DoubleInteger>
{
    private Double distance = 0.0;
    private Integer type = null;

    public void set(Double lhs, Integer rhs)
    {
        distance = lhs;
        type = rhs;
    }
    
    public Double getDistance()
    {
        return distance;
    }
    
    public Integer getType()
    {
        return type;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException
    {
        distance = in.readDouble();
        type = in.readInt();
    }
    
    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeDouble(distance);
        out.writeInt(type);
    }
    
    @Override
    public int compareTo(DoubleInteger o)
    {
        return (this.type).compareTo(o.type); // TODO why not compare distance?
    }

    @Override
    public String toString() {
        return Double.toString(distance) + " " + Integer.toString(type);
    }
}