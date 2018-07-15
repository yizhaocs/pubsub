package com.adara.hackathon.sub;

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
import java.util.ArrayList;
import java.util.List;

public class JavaSub {

    // use the default project id
    private static String PROJECT_ID = ServiceOptions.getDefaultProjectId();

//    private static final BlockingQueue<PubsubMessage> messages = new LinkedBlockingDeque<>();
    private static final List<PubsubMessage> messages = new ArrayList<>();

    private static GoogleCredentials credentials;

    private static BQWriter bqWriter = new BQWriter();

    public JavaSub() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("adara-spore-drive-2a0537ed329b.json").getFile());
        credentials = GoogleCredentials.fromStream(new FileInputStream(file));
    }

    static class MessageReceiverExample implements MessageReceiver {

        @Override
        public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
            messages.add(message);
            consumer.ack();
        }
    }

    /** Receive messages over a subscription. */
    public static void main(String... args) throws Exception {
        // set subscriber id, eg. my-sub
        String subscriptionId = args[0];
        Boolean turnOnBQ = "yes".equals(args[1]);

        System.out.println("start subscriber");
        System.out.println("sub id is: " + subscriptionId);
        System.out.println("we will write to BQ? " + turnOnBQ);
        System.out.println("project id is : " + PROJECT_ID);
        //System.out.println(System.getenv("GOOGLE_APPLICATION_CREDENTIALS"));

        if (PROJECT_ID == null) {
            PROJECT_ID = "adara-spore-drive";
        }

        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(
                PROJECT_ID, subscriptionId);
        System.out.println("subscriptionName is: " + subscriptionName);

        Subscriber subscriber = null;
        try {
            // create a subscriber bound to the asynchronous message receiver
            subscriber =
                    Subscriber.newBuilder(subscriptionName, new MessageReceiverExample())
                            .setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build();
            System.out.println("subscriber is : " + subscriber);

            subscriber.startAsync().awaitRunning();
            // Continue to listen to messages
            while (true) {
                if (messages.size()>0) {
                    PubsubMessage message = messages.get(0);
                    System.out.println("Message Id: " + message.getMessageId());
                    System.out.println("Data: " + message.getData().toStringUtf8());

                    String content = message.getData().toStringUtf8();

                    // stream the content to BQ
                    if (turnOnBQ)
                        bqWriter.streamDataToBQ(content);
                }
            }
        } finally {
            if (subscriber != null) {
                subscriber.stopAsync();
            }
        }
    }
}