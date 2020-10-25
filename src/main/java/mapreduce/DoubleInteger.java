package mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

// WritableComparable class for a paired Double and String (distance and type)
// This is a custom class for MapReduce to pass a double and a String through context
// as one serializable object.
// This example only implements the minimum required methods to make this job run. To be
// deployed robustly is should include ToString(), hashCode(), WritableComparable interface
// if this object was intended to be used as a key etc.
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
}