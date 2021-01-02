import org.apache.commons.io.FileUtils;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Locale;

public class Main {
  private static final Region REGION = Region.US_EAST_1;
  private static final String HEB_3GRAM_CORPUS =
      "s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data";
  private static final String USER_FILE_PATH = "inputs.txt";

  private static String inBucket; // Get from user file
  private static String inputJarName; // Get from user file
  private static String outBucket; // Get from user file
  private static int instanceCount; // Get from user file
  private static boolean workersLocalAggregation; // Get from user file
  private static boolean outputSingleFile; // Get from user file

  private static S3Client s3;

  public static void main(String[] args) throws InterruptedException {
    s3 = S3Client.builder().region(REGION).build();
    EmrClient emr = EmrClient.builder().region(REGION).build();

    if (readUserFile()) {
      uploadJARToBucket();

      HadoopJarStepConfig hadoopJarStep =
          HadoopJarStepConfig.builder()
              .jar(String.format("s3n://%s/%s", inBucket, inputJarName))
              .mainClass("WordPrediction.WordPredictionRunner")
              .args(
                  HEB_3GRAM_CORPUS,
                  String.format("s3n://%s/output/", outBucket),
                  String.valueOf(workersLocalAggregation),
                  String.valueOf(outputSingleFile))
              .build();

      StepConfig stepConfig =
          StepConfig.builder()
              .name("Hebrew 3Gram Calculate Deleted Estimations")
              .hadoopJarStep(hadoopJarStep)
              .actionOnFailure("TERMINATE_JOB_FLOW")
              .build();

      JobFlowInstancesConfig instances =
          JobFlowInstancesConfig.builder()
              .instanceCount(instanceCount)
              .masterInstanceType(InstanceType.M4_LARGE.toString())
              .slaveInstanceType(InstanceType.M4_LARGE.toString())
              .hadoopVersion("3.2.1")
              .ec2KeyName("itay")
              .keepJobFlowAliveWhenNoSteps(false)
              .placement(PlacementType.builder().availabilityZone("us-east-1a").build())
              .build();

      RunJobFlowRequest runFlowRequest =
          RunJobFlowRequest.builder()
              .name("Hebrew 3Gram Deleted Estimation")
              .instances(instances)
              .steps(stepConfig)
              .releaseLabel("emr-6.2.0")
              .jobFlowRole("EMR_EC2_DefaultRole")
              .serviceRole("EMR_DefaultRole")
              .logUri(String.format("s3n://%s/log-files/", outBucket))
              .build();

      RunJobFlowResponse runFlowResponse = emr.runJobFlow(runFlowRequest);
      String jobFlowId = runFlowResponse.jobFlowId();
      System.out.printf(
          "\n\nHebrew 3Gram Deleted Estimation job started with job ID: %s%n", jobFlowId);

      while (true) {
        DescribeClusterRequest clusterRequest =
            DescribeClusterRequest.builder().clusterId(jobFlowId).build();
        DescribeClusterResponse clusterResponse = emr.describeCluster(clusterRequest);
        ClusterState jobState = clusterResponse.cluster().status().state();

        System.out.println("Job Status: " + jobState);
        switch (jobState) {
          case TERMINATED_WITH_ERRORS:
            System.err.println(
                "Job terminated with errors. Downloading log files to output directory.\n");
            downloadOutputBucket(jobFlowId, true);
            deleteOutputBucket();
            System.exit(1);
          case TERMINATING:
          case TERMINATED:
            System.out.println(
                "Job completed successfully, downloading results output and log files.\n");
            downloadOutputBucket(jobFlowId, false);
            deleteOutputBucket();
            System.exit(0);
        }
        Thread.sleep(5000);
      }
    }
  }

  public static void uploadJARToBucket() {
    System.out.println("Uploading JAR file to S3 bucket: " + inBucket);
    try {
      PutObjectRequest putObjectRequest =
              PutObjectRequest.builder()
                      .bucket(outBucket)
                      .key(inputJarName)
                      .acl(ObjectCannedACL.PUBLIC_READ_WRITE)
                      .build();
      s3.putObject(putObjectRequest, Paths.get(inputJarName));
    } catch (Exception e) {
      System.err.println("Failed to upload JAR file to provided S3 bucket.");
    }
    System.out.println("Finished Uploading JAR file.");
  }

  public static void downloadOutputBucket(String jobId, boolean onlyLog) {
    File outDir = new File("output");
    if (!outDir.isDirectory()) {
      outDir.mkdir();
    }
    if (!onlyLog) {
      System.out.println("Downloading output files from S3");
      ListObjectsV2Request outputListObjectRequest =
          ListObjectsV2Request.builder().bucket(outBucket).prefix("output/result").build();
      downloadBucketDirectory(outputListObjectRequest, false);
    }

    System.out.println("Download files completed.\nOutput is in the output folder created.");

    System.out.println("Downloading log files from S3");
    ListObjectsV2Request logListObjectRequest =
        ListObjectsV2Request.builder()
            .bucket(outBucket)
            .prefix(String.format("log-files/%s/steps", jobId))
            .build();
    downloadBucketDirectory(logListObjectRequest, true);
  }

  private static void downloadBucketDirectory(ListObjectsV2Request request, boolean addOutput) {
    ListObjectsV2Iterable listObjectResponse = s3.listObjectsV2Paginator(request);
    listObjectResponse.stream()
        .forEach(
            page ->
                page.contents()
                    .forEach(
                        object -> {
                          if (object.size() > 0) {
                            System.out.printf(
                                "Downlading file to %s%s%n",
                                addOutput ? "output/" : "", object.key());
                            InputStream stream = getFileStream(outBucket, object.key());
                            File file =
                                new File(
                                    String.format(
                                        "%s%s", addOutput ? "output/" : "", object.key()));
                            try {
                              assert stream != null;
                              FileUtils.copyInputStreamToFile(stream, file);
                            } catch (IOException e) {
                              e.printStackTrace();
                            }
                          }
                        }));
  }

  public static InputStream getFileStream(String bucketName, String key) {
    try {
      GetObjectRequest objectRequest =
          GetObjectRequest.builder().bucket(bucketName).key(key).build();
      return s3.getObject(objectRequest, ResponseTransformer.toBytes()).asInputStream();
    } catch (Exception e) {
      return null;
    }
  }

  public static void deleteObject(String bucketName, String key) {
    DeleteObjectRequest deleteObjectRequest =
        DeleteObjectRequest.builder().bucket(bucketName).key(key).build();
    s3.deleteObject(deleteObjectRequest);
  }

  public static void deleteOutputBucket() {
    System.out.println("Starting output bucket deletion.");
    try {
      ListObjectsV2Request listObjectRequest =
          ListObjectsV2Request.builder().bucket(outBucket).build();
      ListObjectsV2Iterable listObjectResponse = s3.listObjectsV2Paginator(listObjectRequest);
      listObjectResponse.stream()
          .forEach(
              page ->
                  page.contents()
                      .forEach(
                          object -> {
                            System.out.printf("Deleting file %s%n", object.key());
                            deleteObject(outBucket, object.key());
                          }));
      System.out.printf("Deleting bucket %s%n", outBucket);
      DeleteBucketRequest deleteBucketRequest =
          DeleteBucketRequest.builder().bucket(outBucket).build();
      s3.deleteBucket(deleteBucketRequest);
    } catch (Exception e) {
      System.err.println("Delete output bucket failed.");
      e.printStackTrace();
    }
  }

  private static boolean readUserFile() {
    File userFile = new File(USER_FILE_PATH);

    try (BufferedReader reader = new BufferedReader(new FileReader(userFile))) {
      String input = reader.readLine();
      String[] inputDetails = input.split(" ");
      inBucket = inputDetails[0];
      inputJarName = inputDetails[1];
      outBucket = reader.readLine();
      instanceCount = Integer.parseInt(reader.readLine());
      if (instanceCount <= 0 || instanceCount >= 10) {
        System.err.println(
            "Illegal instance count provided, legal instance count values are in range 0 < <instance-count> < 10");
        return false;
      }
      String localAggregation = reader.readLine().toLowerCase(Locale.ROOT);
      if (!localAggregation.equals("true") && !localAggregation.equals("false")) {
        System.err.println("Local aggregation indicator must have a true or false value.");
        throw new Exception();
      }
      workersLocalAggregation = Boolean.parseBoolean(localAggregation);

      String singleOutput = reader.readLine().toLowerCase(Locale.ROOT);
      if (!singleOutput.equals("true") && !singleOutput.equals("false")) {
        System.err.println("Single output indicator must have a true or false value.");
        throw new Exception();
      }
      outputSingleFile = Boolean.parseBoolean(singleOutput);
      return true;
    } catch (Exception e) {
      System.err.println(
          "\nIllegal user file format.\nExpected the following input file format:\n\n<input-bucket> <input-jar-file-name>\n<output-bucket>\n<worker-instance-count>\n<use-local-aggregation>\n<single-file>.");
      return false;
    }
  }
}
