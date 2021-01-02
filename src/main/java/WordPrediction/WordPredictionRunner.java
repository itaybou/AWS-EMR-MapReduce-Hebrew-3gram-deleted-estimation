package WordPrediction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordPredictionRunner {
  private static String inputCorpusPath;
  private static String outputBucketPath;
  private static boolean useLocalAggregation;
  private static boolean outputSingleFile;
  private static final String LOG_PATH = "/log-files/";

  private static long N;

  public static void main(String[] args) throws IOException {

    if (args.length < 2) {
      System.err.println(
          "Wrong argument count received.\nExpected <input-corpus-path> <output-s3-path>.");
      System.exit(1);
    }

    inputCorpusPath = args[0];
    outputBucketPath = args[1];
    useLocalAggregation = Boolean.parseBoolean(args[2]);
    outputSingleFile = Boolean.parseBoolean(args[3]);

    // Split Corpus
    Configuration splitCorpusConfig = new Configuration();
    final Job splitCorpus = Job.getInstance(splitCorpusConfig, "Split Corpus");
    String splitCorpusPath = createSplitCorpusJob(splitCorpus, inputCorpusPath);
    waitForJobCompletion(splitCorpus, splitCorpusPath);

    Counters counters = splitCorpus.getCounters();
    Counter counter = counters.findCounter(CounterTypes.NGRAMS_COUNTER);
    N = counter.getValue();

    // Aggregate Nr0 and Nr1 values required for deleted estimation calculation
    Configuration jobNrConfig = new Configuration();
    jobNrConfig.setEnum("operation", Operations.NR);
    final Job aggregateNr = Job.getInstance(jobNrConfig, "Aggregate Nr");
    String aggregatedNrPath = createNrTrJob(aggregateNr, splitCorpusPath);
    waitForJobCompletion(aggregateNr, aggregatedNrPath);

    // Aggregate Tr01 and Tr10 values required for deleted estimation calculation
    Configuration jobTrConfig = new Configuration();
    jobTrConfig.setEnum("operation", Operations.TR);
    final Job aggregateTr = Job.getInstance(jobTrConfig, "Aggregate Tr");
    String aggregatedTrPath = createNrTrJob(aggregateTr, splitCorpusPath);
    waitForJobCompletion(aggregateTr, aggregatedTrPath);

    // Join Nr and Tr aggregated values with the corresponding hebrew 3Grams
    Configuration joinThreeGramNrTrThreeGramsConfig = new Configuration();
    final Job joinThreeGramNrTrThreeGrams =
        Job.getInstance(joinThreeGramNrTrThreeGramsConfig, "Join Nr Tr With 3Grams");
    String joinedThreeGramNrTrThreeGramsPath =
        createJoinThreeGramNrTrJob(
            joinThreeGramNrTrThreeGrams, splitCorpusPath, aggregatedNrPath, aggregatedTrPath);
    waitForJobCompletion(joinThreeGramNrTrThreeGrams, joinedThreeGramNrTrThreeGramsPath);

    // Calculate deleted estimation: (Tr01 + Tr10) / (N * (Nr0 + Nr1))
    Configuration jobCalculateDeletedEstimationsConfig = new Configuration();
    jobCalculateDeletedEstimationsConfig.setLong("N", N);
    final Job calculateDeletedEstimationsJob =
        Job.getInstance(jobCalculateDeletedEstimationsConfig, "Calculate Deleted Estimations");
    String calculateDeletedEstimationsPath =
        createDeletedEstimationJob(
            calculateDeletedEstimationsJob, joinedThreeGramNrTrThreeGramsPath);
    waitForJobCompletion(calculateDeletedEstimationsJob, calculateDeletedEstimationsPath);

    // Sort deleted estimation by first two words from 3Gram and break ties with the deleted
    // estimation probability value
    Configuration sortThreeGramsConfig = new Configuration();
    final Job sortThreeGramsJob = Job.getInstance(sortThreeGramsConfig, "Sort Deleted Estimations");
    String sortPath = SortJobMaker(sortThreeGramsJob, calculateDeletedEstimationsPath);
    waitForJobCompletion(sortThreeGramsJob, sortPath);

    System.out.printf(
        "\nFinished all jobs successfully: output can be found in s3 path: %s%n",
        String.format("%s/result", outputBucketPath));
  }

  private static String setInputOutput(Job job, String inputPath, boolean finished)
      throws IOException {
    FileInputFormat.addInputPath(job, new Path(inputPath));
    String outputPath =
        finished
            ? String.format("%s/result", outputBucketPath)
            : String.format("%s/jobs/%s", outputBucketPath, job.getJobName());
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    return outputPath;
  }

  private static String setOutput(Job job) {
    String outputPath = String.format("%s/jobs/%s", outputBucketPath, job.getJobName());
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    return outputPath;
  }

  private static String createSplitCorpusJob(Job job, String filePath) throws IOException {
    job.setJarByClass(SplitCorpus.class);
    job.setMapperClass(SplitCorpus.MapperClass.class);
    job.setPartitionerClass(SplitCorpus.PartitionerClass.class);
    if (useLocalAggregation) {
      job.setCombinerClass(SplitCorpus.CombinerClass.class); // Split Corpus Combiner
    }
    job.setReducerClass(SplitCorpus.ReducerClass.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongPairWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongPairWritable.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    return setInputOutput(job, filePath, false);
  }

  private static String createNrTrJob(Job job, String filePath) throws IOException {
    job.setJarByClass(AggregateNrTr.class);
    job.setMapperClass(AggregateNrTr.MapperClass.class);
    job.setPartitionerClass(AggregateNrTr.PartitionerClass.class);
    if (useLocalAggregation) {
      job.setCombinerClass(AggregateNrTr.CombinerClass.class); // Aggregate Nr Tr Combiner
    }
    job.setReducerClass(AggregateNrTr.ReducerClass.class);
    job.setMapOutputKeyClass(TextLongPairWritable.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(TextLongPairWritable.class);
    return setInputOutput(job, filePath, false);
  }

  private static String createJoinThreeGramNrTrJob(
      Job job, String splitCorpusPath, String aggregatedNrPath, String aggregatedTrPath) {
    job.setJarByClass(CalculateDeletedEstimations.class);
    job.setReducerClass(JoinNrTrThreeGrams.ReducerClass.class);
    job.setPartitionerClass(JoinNrTrThreeGrams.PartitionerClass.class);
    job.setSortComparatorClass(JoinNrTrThreeGrams.JoinThreeGramNrTrSortComparator.class);
    job.setGroupingComparatorClass(JoinNrTrThreeGrams.JoinThreeGramNrTrGroupingComparator.class);
    job.setMapOutputKeyClass(OccurrencesThreeGramComparable.class);
    job.setMapOutputValueClass(TextPairWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(TextLongPairWritable.class);
    MultipleInputs.addInputPath(
        job,
        new Path(splitCorpusPath),
        TextInputFormat.class,
        JoinNrTrThreeGrams.ThreeGramMapperClass.class);
    MultipleInputs.addInputPath(
        job,
        new Path(aggregatedNrPath),
        TextInputFormat.class,
        JoinNrTrThreeGrams.NrTrMapperClass.class);
    MultipleInputs.addInputPath(
        job,
        new Path(aggregatedTrPath),
        TextInputFormat.class,
        JoinNrTrThreeGrams.NrTrMapperClass.class);
    return setOutput(job);
  }

  private static String createDeletedEstimationJob(Job job, String joinThreeGramNrTrThreeGramsPath)
      throws IOException {
    job.setJarByClass(CalculateDeletedEstimations.class);
    job.setMapperClass(CalculateDeletedEstimations.MapperClass.class);
    job.setReducerClass(CalculateDeletedEstimations.ReducerClass.class);
    job.setPartitionerClass(CalculateDeletedEstimations.PartitionerClass.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(TextLongPairWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    return setInputOutput(job, joinThreeGramNrTrThreeGramsPath, false);
  }

  private static String SortJobMaker(Job job, String deletedEstimationsPath) throws IOException {
    job.setJarByClass(SortDeletedEstimations.class);
    job.setMapperClass(SortDeletedEstimations.MapperClass.class);
    if (outputSingleFile) {
      job.setPartitionerClass(SortDeletedEstimations.SingleFilePartitionerClass.class);
    } else job.setPartitionerClass(SortDeletedEstimations.PartitionerClass.class);
    job.setSortComparatorClass(SortDeletedEstimations.ProbabilityThreeGramComparator.class);
    job.setReducerClass(SortDeletedEstimations.ReducerClass.class);
    job.setMapOutputKeyClass(ProbabilityThreeGramComparable.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setOutputKeyClass(ProbabilityThreeGramComparable.class);
    job.setOutputValueClass(NullWritable.class);
    return setInputOutput(job, deletedEstimationsPath, true);
  }

  private static void waitForJobCompletion(final Job job, String outputPath) {
    String description = job.getJobName();
    System.out.printf("Started %s job.%n", description);
    try {
      if (job.waitForCompletion(true)) {
        System.out.printf(
            "%s finished successfully, output in S3 bucket %s.%n", description, outputPath);
      } else {
        System.out.printf("%s failed!, logs in S3 bucket at %s.%n", description, LOG_PATH);
        System.exit(1);
      }
    } catch (InterruptedException | IOException | ClassNotFoundException e) {
      System.err.printf(
          "Exception caught! EXCEPTION: %s\nLogs in S3 bucket at %s.%n", e.getMessage(), LOG_PATH);
      System.exit(1);
    }
  }
}
