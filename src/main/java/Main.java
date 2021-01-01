import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class Main {
    private static final Region REGION = Region.US_EAST_1;
    private static final String HEB_3GRAM_CORPUS =
            "s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data";
    private static final String USER_FILE_PATH = "inputs.txt";

    private static String inBucket; // Get from user file
    private  static String inputJarName; // Get from user file
    private static String outBucket; // Get from user file
    private static int instanceCount; // Get from user file

    public static void main(String[] args) {
        EmrClient emr = EmrClient.builder().region(REGION).build();

        if(readUserFile()) {
            HadoopJarStepConfig hadoopJarStep =
                    HadoopJarStepConfig.builder()
                            .jar(String.format("s3n://%s/%s", inBucket, inputJarName))
                            .mainClass("WordPrediction.WordPredictionRunner")
                            .args(HEB_3GRAM_CORPUS, String.format("s3n://%s/output/", outBucket))
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
            System.out.printf("Hebrew 3Gram Deleted Estimation job started with job ID: %s%n", jobFlowId);
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
            return true;
        } catch (IOException e) {
            System.err.println(
                    "Illegal user file format.\nExpected the following input file format:\n<input-bucket> <input-jar-file-name>\n<output-bucket>.");
            return false;
        }
    }
}
