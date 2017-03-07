package com.github.nachomezzadra.sample;


import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class SqsTest extends BaseSpringTest {

    @Test
    public void shouldProperlyGrantPermissions() {
        AmazonSQS sqs = getAmazonSQS();
        String awsKey = "399265524741";
        String queueName = "nacho-test";

        AddPermissionRequest addPermissionRequest = new AddPermissionRequest();
        addPermissionRequest.setQueueUrl(sqs.getQueueUrl(queueName).getQueueUrl());
        addPermissionRequest.setActions(Arrays.asList("SendMessage", "ReceiveMessage", "GetQueueUrl", "DeleteMessage", "GetQueueAttributes"));
        addPermissionRequest.setLabel("Some Tenant");
        addPermissionRequest.setAWSAccountIds(Arrays.asList(awsKey));
        AddPermissionResult addPermissionResult = sqs.addPermission(addPermissionRequest);

        assertNotNull(addPermissionResult);
    }


    @Test
    public void shouldProperlyRevokePermissions() {
        AmazonSQS sqs = getAmazonSQS();
        String queueName = "nacho-test";

        RemovePermissionRequest removePermissionRequest = new RemovePermissionRequest();
        removePermissionRequest.setLabel("Some Tenant");
        removePermissionRequest.setQueueUrl(sqs.getQueueUrl(queueName).getQueueUrl());
        RemovePermissionResult removePermissionResult = sqs.removePermission(removePermissionRequest);

        assertNotNull(removePermissionResult);
    }


    @Test
    public void shoudlProperlyCreateAnSqs() {
        AmazonSQS sqs = getAmazonSQS();

        System.out.println("===========================================");
        System.out.println("Getting Started with Amazon SQS");
        System.out.println("===========================================\n");

        String queueName = "nacho-test";

        try {
            // Create a queue
            System.out.println("Creating a new SQS queue called " + queueName + "\n");
            CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
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
        BasicAWSCredentials credentials = new BasicAWSCredentials("AKIQI63B4XDOKYS3SAYA",
                "ssNtWHNcrCUD3HXaqbCxy/AQ+t4yVJ2MX+oB5Y9z");
        AwsClientBuilder.EndpointConfiguration endpointConfiguration = new AwsClientBuilder.EndpointConfiguration("https://sqs.us-west-2.amazonaws.com",
                "us-west-2");

        ClientConfiguration config = new ClientConfiguration();
        AmazonSQS sqsNew = AmazonSQSClientBuilder.standard().
                withCredentials(new AWSStaticCredentialsProvider(credentials)).
                withEndpointConfiguration(endpointConfiguration).
                build();

        return sqsNew;
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


    @Test
    public void shouldProperlyGetSqsRequestParameters() {
        AmazonSQS sqs = getAmazonSQS();
        GetQueueAttributesRequest getQRequest = new GetQueueAttributesRequest();
        getQRequest.setQueueUrl("http://0.0.0.0:4568/nacho-test");
        Map<String, String> attributes = sqs.getQueueAttributes(getQRequest).getAttributes();
//        List<String> attributeNames = getQRequest.getAttributeNames();
        System.out.println("Attributes: " + attributes);
//        for (Map.Entry<String, String> eachAttribute : attributes) {
//            System.out.println("Attribute: " + eachAttribute + "\n");
//        }


    }


}
