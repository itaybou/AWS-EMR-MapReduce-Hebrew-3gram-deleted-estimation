package WordPrediction;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class LongPairWritable implements WritableComparable<LongPairWritable> {

    public LongWritable first;
    public LongWritable second;

    public LongPairWritable() {
        first = new LongWritable(0);
        second = new LongWritable(0);
    }

    public LongPairWritable(LongWritable first, LongWritable second) {
        this.first = first;
        this.second = second;
    }

    public LongWritable getFirst() {
        return first;
    }

    public LongWritable getSecond() {
        return second;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }

    @Override
    public int compareTo(LongPairWritable o) {
        return this.first.compareTo(o.first) != 0
                ? this.first.compareTo(o.first)
                : this.second.compareTo(o.second);
    }

    @Override
    public String toString() {
        return String.format("%d\t%d", first.get(), second.get());
    }

    @Override
    public int hashCode() {
      return Objects.hash(first.get(), second.get());
    }
}
