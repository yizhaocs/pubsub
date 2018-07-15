package com.adara.hackathon.pub;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Logger;

public class PubTask implements Runnable {
    private static final Logger log = Logger.getLogger(PubTask.class.getName());
    private Publisher publisher;
    public PubTask(Publisher publisher) {
        this.publisher = publisher;
    }

    public void run() {
        // construct a pubsub message from the payload

        String data = getUnixTimeStamp() + "|" +  getUnixTimeStamp() + "|" + "hostname:" + getHostName() + ", current time:" + getCurrentDateTime();
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(data)).build();

        ApiFuture<String> future = publisher.publish(pubsubMessage);
        // Add an asynchronous callback to handle success / failure
        ApiFutures.addCallback(future, new ApiFutureCallback<String>() {

            @Override
            public void onFailure(Throwable throwable) {
                if (throwable instanceof ApiException) {
                    ApiException apiException = ((ApiException) throwable);
                    // details on the API exception
                    System.out.println("apiException.getStatusCode().getCode():" + apiException.getStatusCode().getCode());
                    System.out.println("apiException.isRetryable():" + apiException.isRetryable());
                }
                System.out.println("Error publishing message : " + data);
                log.info("[PubTask.run]" + "Error publishing message : " + data);
            }

            @Override
            public void onSuccess(String messageId) {
                // Once published, returns server-assigned message ids (unique within the topic)
                System.out.println("onSuccess with messageId:" + messageId + " , data:" + data);
                log.info("[PubTask.run]" + "onSuccess with messageId:" + messageId + " , data:" + data);
            }
        });
    }

    private String getCurrentDateTime() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        return dateFormat.format(date);
    }

    public String generateRandomUuid() {
        return java.util.UUID.randomUUID().toString();
    }

    public static String getUnixTimeStamp(){
        return String.valueOf(System.currentTimeMillis()/1000);

    }

    public String getHostName() {
        String hostname = "Unknown";
        try {
            InetAddress addr;
            addr = InetAddress.getLocalHost();
            hostname = addr.getHostName();
        } catch (UnknownHostException ex) {
        }
        return hostname;
    }

    public static void main(String[] args){
        System.out.println(java.util.UUID.randomUUID().toString());
    }
}