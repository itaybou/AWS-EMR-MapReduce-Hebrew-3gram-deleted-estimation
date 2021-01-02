package WordPrediction;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ProbabilityThreeGramComparable implements WritableComparable<ProbabilityThreeGramComparable> {

    private Text threeGram;
    private DoubleWritable probability;

    public ProbabilityThreeGramComparable() {
        threeGram = new Text();
        probability = new DoubleWritable(0f);
    }

    public ProbabilityThreeGramComparable(Text threeGram, DoubleWritable probability) {
        this.threeGram = threeGram;
        this.probability = probability;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(toString());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String line = dataInput.readUTF();
        String[] splitted = line.split("\t");
        threeGram = new Text(splitted[0]);
        probability = new DoubleWritable(Double.parseDouble(splitted[1]));
    }

    public String toString() {
        return threeGram.toString() + '\t' + probability.get();
    }

    @Override
    public int compareTo(ProbabilityThreeGramComparable other) {
        String otherThreeGram = other.getThreeGram().toString();
        String[] words = threeGram.toString().split(" ");
        String[] otherWords = otherThreeGram.split(" ");
        Double otherProbability = other.getProbability().get();

        int firstComparison = words[0].compareTo(otherWords[0]);
        int secondComparison = words[1].compareTo(otherWords[1]);
        int probComparison = otherProbability.compareTo(probability.get());
        return firstComparison != 0 ? firstComparison : secondComparison != 0 ? secondComparison : probComparison;
    }

    @Override
    public int hashCode() {
        String[] splitted = threeGram.toString().split(" ");
        String twoWords = String.format("%s %s", splitted[0], splitted[1]);
        return twoWords.hashCode();
    }

    public Text getThreeGram() {
        return threeGram;
    }

    public DoubleWritable getProbability() {
        return probability;
    }
}


