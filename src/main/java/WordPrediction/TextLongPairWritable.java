package WordPrediction;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class TextLongPairWritable implements WritableComparable<TextLongPairWritable> {
    public Text first;
    public LongWritable second;

    public TextLongPairWritable() {
        first = new Text();
        second = new LongWritable(0);
    }

    public TextLongPairWritable(Text first, LongWritable second){
        this.first = first;
        this.second = second;

    }

    public Text getFirst() {
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

    public String toString() {
        return String.format("%s\t%d", first.toString(), second.get());
    }

    @Override
    public int compareTo(TextLongPairWritable o) {
        return this.first.compareTo(o.first) != 0
                ? this.first.compareTo(o.first)
                : this.second.compareTo(o.second);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first.toString(), second.get());
    }
}
