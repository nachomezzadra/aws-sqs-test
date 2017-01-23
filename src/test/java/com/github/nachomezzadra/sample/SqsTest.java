package com.github.nachomezzadra.sample;


import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class SqsTest extends BaseSpringTest {

    @Test
    public void shoudlProperlyCreateAnSqs() {
        AmazonSQS sqs = getAmazonSQS();

        System.out.println("===========================================");
        System.out.println("Getting Started with Amazon SQS");
        System.out.println("===========================================\n");

        try {
            // Create a queue
            System.out.println("Creating a new SQS queue called MyQueue.\n");
            CreateQueueRequest createQueueRequest = new CreateQueueRequest("nacho-test");
            String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();

            // List queues
            System.out.println("Listing all queues in your account from: " + myQueueUrl + ".\n");
            for (String queueUrl : sqs.listQueues().getQueueUrls()) {
                System.out.println("  QueueUrl: " + queueUrl);
            }
            System.out.println();


        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it " +
                    "to Amazon SQS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered " +
                    "a serious internal problem while trying to communicate with SQS, such as not " +
                    "being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

    private AmazonSQS getAmazonSQS() {
//        AWSCredentials credentials = null;
        AWSCredentials credentials = new AWSCredentials() {
            public String getAWSAccessKeyId() {
                return "AKIAJTHP46QFQDQJWD4A";
            }

            public String getAWSSecretKey() {
                return "uR+FLVDRLWO1RVy4Hzn7YojeUA1kI7Zl7y0cTaL4";
            }
        };
        try {

//            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (~/.aws/credentials), and is in valid format.",
                    e);
        }

        AmazonSQS sqs = new AmazonSQSClient(credentials);
        Region usWest2 = Region.getRegion(Regions.US_EAST_1);
//        sqs.setRegion(usWest2);
//        sqs.setEndpoint("http://localhost:4568");
        return sqs;
    }


    @Test
    public void shouldProperlySendAMessageToSqs() {
        AmazonSQS sqs = getAmazonSQS();
        GetQueueUrlRequest getQRequest = new GetQueueUrlRequest();
        getQRequest.setQueueName("nacho-test");
        String myQueueUrl = sqs.getQueueUrl(getQRequest).getQueueUrl();

        // Send a message
        System.out.println("Sending a message to MyQueue.\n");
        sqs.sendMessage(new SendMessageRequest(myQueueUrl, "This is my message text."));
    }

    @Test
    public void shouldProperlyReadMessagesFromSQS() {
        AmazonSQS sqs = getAmazonSQS();
        GetQueueUrlRequest getQRequest = new GetQueueUrlRequest();
        getQRequest.setQueueName("nacho-test");
        String myQueueUrl = sqs.getQueueUrl(getQRequest).getQueueUrl();
        // Receive messages
        System.out.println("Receiving messages from MyQueue.\n");
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        for (Message message : messages) {
            System.out.println("  Message");
            System.out.println("    MessageId:     " + message.getMessageId());
            System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
            System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
            System.out.println("    Body:          " + message.getBody());
            for (Map.Entry<String, String> entry : message.getAttributes().entrySet()) {
                System.out.println("  Attribute");
                System.out.println("    Name:  " + entry.getKey());
                System.out.println("    Value: " + entry.getValue());
            }
        }
        System.out.println();

        // Delete a message
        System.out.println("Deleting a message.\n");
        String messageReceiptHandle = messages.get(0).getReceiptHandle();
        sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageReceiptHandle));
    }


    @Test
    public void shouldProperlyDeleteQueue() {
        AmazonSQS sqs = getAmazonSQS();
        GetQueueUrlRequest getQRequest = new GetQueueUrlRequest();
        getQRequest.setQueueName("nacho-test");
        String myQueueUrl = sqs.getQueueUrl(getQRequest).getQueueUrl();

        // Delete a queue
        System.out.println("Deleting the test queue.\n");
        sqs.deleteQueue(new DeleteQueueRequest(myQueueUrl));
    }
}
