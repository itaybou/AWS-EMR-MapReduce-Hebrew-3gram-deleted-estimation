package WordPrediction;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class TextPairWritable implements WritableComparable<TextPairWritable> {
    public Text first;
    public Text second;

    public TextPairWritable() {
        first = new Text();
        second = new Text();
    }

    public TextPairWritable(Text first, Text second){
        this.first = first;
        this.second = second;
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
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
        return String.format("%s\t%s", first.toString(), second.toString());
    }

    @Override
    public int compareTo(TextPairWritable o) {
        return this.first.compareTo(o.first) != 0
                ? this.first.compareTo(o.first)
                : this.second.compareTo(o.second);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first.toString(), second.toString());
    }
}
