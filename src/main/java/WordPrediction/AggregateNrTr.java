package WordPrediction;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AggregateNrTr {
    public static class MapperClass extends Mapper<LongWritable, Text, TextLongPairWritable, LongWritable> {
        private final LongWritable one = new LongWritable(1);
        private Operations operationTag;

        @Override
        public void setup(Context context) {
            operationTag = context.getConfiguration().getEnum("operation", Operations.ILLEGAL);
            if (operationTag == null) {
                System.err.printf("Illegal operation tag value passed: '%s' to Aggregation.%n", Operations.ILLEGAL);
                System.exit(1);
            }
        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] splitted = line.toString().split("\t");
            if (splitted.length == 3) {
                Text threeGram = new Text(splitted[0]);
                LongWritable firstSplitOccurrences = new LongWritable(Long.parseLong(splitted[1]));
                LongWritable secondSplitOccurrences = new LongWritable(Long.parseLong(splitted[2]));
                switch (operationTag) {
                    case NR:
                        context.write(new TextLongPairWritable(new Text(String.format("%s0", operationTag)), firstSplitOccurrences), one);
                        context.write(new TextLongPairWritable(new Text(String.format("%s1", operationTag)), secondSplitOccurrences), one);
                        break;
                    case TR:
                        context.write(new TextLongPairWritable(new Text(String.format("%s01", operationTag)), firstSplitOccurrences), secondSplitOccurrences);
                        context.write(new TextLongPairWritable(new Text(String.format("%s10", operationTag)), secondSplitOccurrences), firstSplitOccurrences);
                        break;
                    case ILLEGAL:
                        System.err.printf("Illegal operation tag value passed: '%s' to Aggregation.%n", Operations.ILLEGAL);
                        System.exit(1);
                }
            }
        }
    }

    public static class CombinerClass extends Reducer<TextLongPairWritable, LongWritable, TextLongPairWritable, LongWritable> {

        @Override
        public void reduce(TextLongPairWritable splitR, Iterable<LongWritable> threeGramsCount, Context context) throws IOException, InterruptedException {
            long intermediateSum = 0;
            String operation = splitR.getFirst().toString();

            // Count 3Grams under the same r value
            for (LongWritable count : threeGramsCount) {
                intermediateSum += count.get(); // Add intermediate sums
            }

            context.write(splitR, new LongWritable(intermediateSum));
        }
    }

    public static class ReducerClass extends Reducer<TextLongPairWritable, LongWritable, LongWritable, TextLongPairWritable> {

        @Override
        public void reduce(TextLongPairWritable splitR, Iterable<LongWritable> threeGramCount, Context context) throws IOException, InterruptedException {
            long sum = 0;
            String operation = splitR.getFirst().toString();

            // Count 3Gram sums under the same r value
            for (LongWritable count : threeGramCount) {
                sum += count.get(); // Add total sums (combiner intermediate sums)
            }

            context.write(splitR.getSecond(), new TextLongPairWritable(new Text(operation), new LongWritable(sum)));
        }
    }

    public static class PartitionerClass extends Partitioner<TextLongPairWritable, LongWritable> {

        @Override
        public int getPartition(TextLongPairWritable key, LongWritable value, int numPartitions) {
            return (key.hashCode() & 0xFFFFFFF) % numPartitions;
        }
    }
}