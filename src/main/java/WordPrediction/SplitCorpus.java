package WordPrediction;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

public class SplitCorpus {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongPairWritable> {

        private static final Pattern HEBREW_3GRAM_REGEX = Pattern.compile("^(?:[א-ת]+[א-ת\\d+ ]+|[א-ת\\d+ ]+[א-ת]+|[א-ת\\d+ ]+[א-ת]+[א-ת\\d+ ]+)$");

        @Override
        public void map(LongWritable lineId, Text line, Context context)
                throws IOException, InterruptedException {
            String[] split = line.toString().split("\t");
            String threeGram = split[0];
            StringTokenizer wordCount = new StringTokenizer(threeGram, " ");

            // Filter out only Hebrew 3Grams consisting only of hebrew alphabet and numbers
            if (wordCount.countTokens() == 3 && HEBREW_3GRAM_REGEX.matcher(threeGram).matches()) {
                LongWritable occurrences = new LongWritable(Long.parseLong(split[2]));
                LongPairWritable pair =
                        new LongPairWritable(
                                new LongWritable(lineId.get() % 2 == 0 ? 0 : 1), // Split corpus by even and odd line numbers to create equal sized splits
                                occurrences);
                context.write(new Text(threeGram), pair);
            }
        }
    }

    public static class CombinerClass extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {

        @Override
        public void reduce(Text line, Iterable<LongPairWritable> pairs, Context context)
                throws IOException, InterruptedException {
            long firstSplitCount = 0, secondSplitCount = 0;
            for (LongPairWritable pair : pairs) {
                LongWritable key = pair.getFirst();
                LongWritable value = pair.getSecond();
                firstSplitCount += key.get() == 0 ? value.get() : 0;
                secondSplitCount += key.get() == 0 ? 0 : value.get();
            }

            context.write(line, new LongPairWritable(new LongWritable(0), new LongWritable(firstSplitCount)));
            context.write(line, new LongPairWritable(new LongWritable(1), new LongWritable(secondSplitCount)));
        }
    }

    public static class ReducerClass extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {
        private Counter ngramsCounter;

        @Override
        public void setup(Context context) {
            ngramsCounter = context.getCounter(CounterTypes.NGRAMS_COUNTER);
        }

        @Override
        public void reduce(Text threeGram, Iterable<LongPairWritable> occurrencesList, Context context)
                throws IOException, InterruptedException {
            long firstSplitCount = 0, secondSplitCount = 0;
            for (LongPairWritable occurrence : occurrencesList) {
                LongWritable key = occurrence.getFirst(); // first split or second split binary key
                LongWritable value = occurrence.getSecond(); // Occurrences of 3Gram
                firstSplitCount += key.get() == 0 ? value.get() : 0;
                secondSplitCount += key.get() == 0 ? 0 : value.get();
            }

            long total = firstSplitCount + secondSplitCount;
            ngramsCounter.increment(total);
            context.write(threeGram, new LongPairWritable(new LongWritable(firstSplitCount), new LongWritable(secondSplitCount)));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongPairWritable> {

        @Override
        public int getPartition(Text key, LongPairWritable value, int numPartitions) {
            return (key.toString().hashCode() & 0xFFFFFFF) % numPartitions;
        }
    }
}
