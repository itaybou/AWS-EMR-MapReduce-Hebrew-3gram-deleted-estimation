package WordPrediction;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CalculateDeletedEstimations {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, TextLongPairWritable> {

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] splitted = line.toString().split("\t");
            if (splitted.length == 3) {
                Text threeGram = new Text(splitted[0]);
                Text operation = new Text(splitted[1]);
                LongWritable value = new LongWritable(Long.parseLong(splitted[2]));
                context.write(threeGram, new TextLongPairWritable(operation, value));
            }
        }
    }

    /**
     * Did not use Combiner in this map reduce job since we perform
     * division operation in order to calculate the deleted estimation probability
     * which is not an associative operation.
     **/
    public static class ReducerClass extends Reducer<Text, TextLongPairWritable, Text, DoubleWritable> {
        private double N;

        public void setup(Context context) {
            N = context.getConfiguration().getLong("N", 1);
        }

        @Override
        public void reduce(Text threeGram, Iterable<TextLongPairWritable> values, Context context) throws IOException, InterruptedException {
            Long Nr0 = -1L, Nr1 = -1L, Tr01 = -1L, Tr10 = -1L;

            // Store Nr0, Nr1, Tr0, Tr1
            for (TextLongPairWritable valueOperationPair : values) {
                String tag = valueOperationPair.getFirst().toString();
                Operations operation = Operations.valueOf(tag.replaceAll("\\d", ""));
                int corpusPart = Character.getNumericValue(tag.replaceAll("[a-zA-Z]", "").charAt(0));
                switch (operation) {
                    case NR:
                        if (corpusPart == 0)
                            Nr0 = valueOperationPair.getSecond().get();
                        else Nr1 = valueOperationPair.getSecond().get();
                        break;
                    case TR:
                        if (corpusPart == 0)
                            Tr01 = valueOperationPair.getSecond().get();
                        else Tr10 = valueOperationPair.getSecond().get();
                        break;
                }
            }

            if (Nr0 != -1 && Nr1 != -1 && Tr01 != -1 && Tr10 != -1) {
                double sumTr = Tr01.doubleValue() + Tr10.doubleValue();
                double sumNr = Nr0.doubleValue() + Nr1.doubleValue();
                double probability = (sumTr / (N * sumNr));
                context.write(threeGram, new DoubleWritable(probability));
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, TextLongPairWritable> {

        @Override
        public int getPartition(Text key, TextLongPairWritable value, int numPartitions) {
            return (key.toString().hashCode() & 0xFFFFFFF) % numPartitions;
        }
    }
}
