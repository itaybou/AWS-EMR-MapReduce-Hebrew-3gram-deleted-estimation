package WordPrediction;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JoinNrTrThreeGrams {
    public static class ThreeGramMapperClass extends Mapper<LongWritable, Text, OccurrencesThreeGramComparable, TextPairWritable> {

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] splitted = line.toString().split("\t");
            if (splitted.length == 3) {
                long firstOccurrences = Long.parseLong(splitted[1]);
                long secondOccurrences = Long.parseLong(splitted[1]);
                Text threeGram = new Text(splitted[0]);
                context.write(new OccurrencesThreeGramComparable(firstOccurrences, OccurrencesThreeGramComparable.JoinComparer.THREE_GRAM),
                        new TextPairWritable(threeGram, new Text(String.format("%s0", Operations.NR))));
                context.write(new OccurrencesThreeGramComparable(secondOccurrences, OccurrencesThreeGramComparable.JoinComparer.THREE_GRAM),
                        new TextPairWritable(threeGram, new Text(String.format("%s1", Operations.NR))));
                context.write(new OccurrencesThreeGramComparable(firstOccurrences, OccurrencesThreeGramComparable.JoinComparer.THREE_GRAM),
                        new TextPairWritable(threeGram, new Text(String.format("%s01", Operations.TR))));
                context.write(new OccurrencesThreeGramComparable(secondOccurrences, OccurrencesThreeGramComparable.JoinComparer.THREE_GRAM),
                        new TextPairWritable(threeGram, new Text(String.format("%s10", Operations.TR))));
            }
        }
    }

    public static class NrTrMapperClass extends Mapper<LongWritable, Text, OccurrencesThreeGramComparable, TextPairWritable> {

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] splitted = line.toString().split("\t");
            if (splitted.length == 3) {
                long occurences = Long.parseLong(splitted[0]);
                Text operation = new Text(splitted[1]);
                Text sumText = new Text(splitted[2]);
                context.write(new OccurrencesThreeGramComparable(occurences, OccurrencesThreeGramComparable.JoinComparer.AGGREGATED), new TextPairWritable(sumText, operation));
            }
        }
    }

    public static class ReducerClass extends Reducer<OccurrencesThreeGramComparable, TextPairWritable, Text, TextLongPairWritable> {

        @Override
        public void reduce(OccurrencesThreeGramComparable splitR, Iterable<TextPairWritable> textPairs, Context context) throws IOException, InterruptedException {
            long Nr0 = -1, Nr1 = -1, Tr01 = -1, Tr10 = -1;

            for (TextPairWritable textPair : textPairs) {
                String[] splitted = textPair.getFirst().toString().split(" ");
                String tag = textPair.getSecond().toString();
                Operations operation = Operations.valueOf(tag.replaceAll("\\d", ""));
                int corpusPart = Character.getNumericValue(tag.replaceAll("[a-zA-Z]", "").charAt(0));

                if (splitted.length == 1) {
                    // Parse input from NrTrMapperClass
                    switch (operation) {
                        case NR:
                            if (corpusPart == 0)
                                Nr0 = Long.parseLong(splitted[0]);
                            else Nr1 = Long.parseLong(splitted[0]);
                            break;
                        case TR:
                            if (corpusPart == 0)
                                Tr01 = Long.parseLong(splitted[0]);
                            else Tr10 = Long.parseLong(splitted[0]);
                            break;
                    }
                } else if (Nr0 != -1 && Nr1 != -1 && Tr01 != -1 && Tr10 != -1) {
                    // Parse input from ThreeGramMapperClass
                    Text threeGram = textPair.getFirst();
                    long sum;
                    if(operation == Operations.NR) {
                        if (corpusPart == 0)
                            sum = Nr0;
                        else sum = Nr1;
                    } else  if (corpusPart == 0)
                        sum = Tr01;
                    else sum = Tr10;
                    context.write(threeGram, new TextLongPairWritable(new Text(tag), new LongWritable(sum)));
                }
            }
        }
    }

    public static class PartitionerClass extends Partitioner<OccurrencesThreeGramComparable, TextPairWritable> {

        @Override
        public int getPartition(OccurrencesThreeGramComparable key, TextPairWritable value, int numPartitions) {
            return (key.hashCode() & 0xFFFFFFF) % numPartitions; // Make sure that equal occurrences will end up in same reducer
        }
    }

    public static class JoinThreeGramNrTrSortComparator extends WritableComparator {

        protected JoinThreeGramNrTrSortComparator() {
            super(OccurrencesThreeGramComparable.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            OccurrencesThreeGramComparable a = (OccurrencesThreeGramComparable) w1;
            OccurrencesThreeGramComparable b = (OccurrencesThreeGramComparable) w2;
            return a.compareTo(b); // Sort the keys so that values from NrTrMapperClass will come before those from ThreeGramMapperClass for every occurrence value
        }
    }

    public static class JoinThreeGramNrTrGroupingComparator extends WritableComparator {
        protected JoinThreeGramNrTrGroupingComparator() {
            super(OccurrencesThreeGramComparable.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            OccurrencesThreeGramComparable a = (OccurrencesThreeGramComparable) w1;
            OccurrencesThreeGramComparable b = (OccurrencesThreeGramComparable) w2;
            return a.getOccurrences().compareTo(b.getOccurrences()); // Group only by the occurrences value so that all with same occurrences will be group to same reducer
        }
    }
}
