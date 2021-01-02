package WordPrediction;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortDeletedEstimations {
    public static class MapperClass extends Mapper<LongWritable, Text, ProbabilityThreeGramComparable, NullWritable> {

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String[] splitted = line.toString().split("\t");
            context.write(new ProbabilityThreeGramComparable(new Text(splitted[0]), new DoubleWritable(Double.parseDouble(splitted[1]))), NullWritable.get());
        }
    }

    public static class ReducerClass extends Reducer<ProbabilityThreeGramComparable, NullWritable, ProbabilityThreeGramComparable, NullWritable> {
        @Override
        public void reduce(ProbabilityThreeGramComparable threeGramProb, Iterable<NullWritable> blanks, Context context)
                throws IOException,  InterruptedException {
            for (NullWritable blank : blanks){
                context.write(threeGramProb, blank);
            }
        }
    }

    public static class PartitionerClass extends Partitioner<ProbabilityThreeGramComparable, NullWritable> {

        @Override
        public int getPartition(ProbabilityThreeGramComparable key, NullWritable value, int numPartitions) {
            return (key.hashCode() & 0xFFFFFFF) % numPartitions;
        }
    }

    public static class SingleFilePartitionerClass extends Partitioner<ProbabilityThreeGramComparable, NullWritable> {

        @Override
        public int getPartition(ProbabilityThreeGramComparable key, NullWritable value, int numPartitions) {
            return 0;
        }
    }

    public static class ProbabilityThreeGramComparator  extends WritableComparator {
        public ProbabilityThreeGramComparator() {
            super(ProbabilityThreeGramComparable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            ProbabilityThreeGramComparable threeGram = (ProbabilityThreeGramComparable) a;
            ProbabilityThreeGramComparable otherThreeGram = (ProbabilityThreeGramComparable) b;
            return threeGram.compareTo(otherThreeGram);
        }
    }
}