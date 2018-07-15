package com.adara.hackathon.sub;

import com.adara.hackathon.pub.PubMain;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;

import java.io.File;
import java.io.FileInputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Logger;

public class SubTask implements Runnable {
    private static final Logger log = Logger.getLogger(SubTask.class.getName());
    // use the default project id
    private static String PROJECT_ID = ServiceOptions.getDefaultProjectId();

    private static BlockingQueue<PubsubMessage> messages = new LinkedBlockingDeque<>();
    //private static final List<PubsubMessage> messages = new ArrayList<>();

    private static GoogleCredentials credentials;
    private BQWriter bqWriter;
    public SubTask(BQWriter bqWriter){
        this.bqWriter = bqWriter;
    }


    public void run() {
        String subscriptionId = "message-worker-sub";
        Boolean turnOnBQ = true;

        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("adara-spore-drive-ba35df9900ab.json").getFile());
        try {
            credentials = GoogleCredentials.fromStream(new FileInputStream(file));
        }catch (Exception e){

        }

        //System.out.println("start subscriber");
        //System.out.println("sub id is: " + subscriptionId);
        //System.out.println("we will write to BQ? " + turnOnBQ);
        //System.out.println("project id is : " + PROJECT_ID);
        //System.out.println(System.getenv("GOOGLE_APPLICATION_CREDENTIALS"));

        if (PROJECT_ID == null) {
            PROJECT_ID = "adara-spore-drive";
        }

        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(
                PROJECT_ID, subscriptionId);
        //System.out.println("subscriptionName is: " + subscriptionName);

        Subscriber subscriber = null;
        try {
            // create a subscriber bound to the asynchronous message receiver
            subscriber =
                    Subscriber.newBuilder(subscriptionName, new MessageReceiverExample())
                            .setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build();
            //System.out.println("subscriber is : " + subscriber);

            subscriber.startAsync().awaitRunning();
            // Continue to listen to messages
            while (true) {
                try {
                    if (messages.size()>0) {
                        PubsubMessage message = messages.poll();
                        //System.out.println("Message Id: " + message.getMessageId());
                        //System.out.println("Data: " + message.getData().toStringUtf8());

                        String content = message.getData().toStringUtf8();

                        // stream the content to BQ
                        if (turnOnBQ)
                            bqWriter.streamDataToBQ(content);

                    }
                }catch(Exception e){
                    // do nothing
                    //System.out.println("bqWriter.streamDataToBQ(content) error");
                    /*
                    if (subscriber != null) {
                        subscriber.stopAsync();
                    }
                    messages = new LinkedBlockingDeque<>();
                    // create a subscriber bound to the asynchronous message receiver
                    subscriber =
                            Subscriber.newBuilder(subscriptionName, new MessageReceiverExample())
                                    .setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build();
                    System.out.println("subscriber is : " + subscriber);

                    subscriber.startAsync().awaitRunning();
*/
                }
            }
        } finally {
            if (subscriber != null) {
                subscriber.stopAsync();
            }
        }
    }

    static class MessageReceiverExample implements MessageReceiver {

        @Override
        public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
            messages.offer(message);
            try {
                consumer.ack();
            }catch (Exception e){

            }
        }
    }

}
