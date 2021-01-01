package WordPrediction;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OccurrencesThreeGramComparable implements WritableComparable<OccurrencesThreeGramComparable> {

    public static enum JoinComparer {
        THREE_GRAM,
        AGGREGATED,
    }

    private JoinComparer joinComparer;
    private Long occurrences;

    public OccurrencesThreeGramComparable() {
        joinComparer = JoinComparer.THREE_GRAM;
        occurrences = 0L;
    }

    public OccurrencesThreeGramComparable(long occurrences, JoinComparer joinComparer) {
        this.occurrences = occurrences;
        this.joinComparer = joinComparer;
    }

    @Override
    public int compareTo(OccurrencesThreeGramComparable other) {
        long occurrencesDiff = occurrences - other.getOccurrences();
        return occurrencesDiff > 0 ? 1 : occurrencesDiff < 0 ? -1 :
                joinComparer.equals(other.joinComparer) ? 0 :
                        joinComparer == JoinComparer.THREE_GRAM ? 1 : -1;
    }

    public void readFields(DataInput dataInput) throws IOException {
        joinComparer = JoinComparer.valueOf(WritableUtils.readString(dataInput));
        occurrences = WritableUtils.readVLong(dataInput);
    }

    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, joinComparer.toString());
        WritableUtils.writeVLong(dataOutput, occurrences);
    }

    @Override
    public int hashCode() {
        return occurrences.hashCode();
    }

    public JoinComparer getJoinComparer() {
        return joinComparer;
    }

    public Long getOccurrences() {
        return occurrences;
    }

    @Override
    public String toString() {
        return String.format("%d\t%s", occurrences, joinComparer);
    }
}
