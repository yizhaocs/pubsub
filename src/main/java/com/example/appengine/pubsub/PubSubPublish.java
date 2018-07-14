/*
 * Copyright 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.appengine.pubsub;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.http.HttpStatus;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * curl  "localhost:8080/pubsub/publish?count=20"
 */
@WebServlet(name = "Publish with PubSub", value = "/pubsub/publish")
public class PubSubPublish extends HttpServlet {
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource("adara-spore-drive-7a12bb7e0cfd.json").getFile());
    //private static final String JSONFILE = "/Users/yzhao/BitBucket03222018/java-docs-samples/appengine-java8/sporedrive/src/main/resources/adara-spore-drive-7a12bb7e0cfd.json";

    @Override
    public void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws IOException, ServletException {

        Publisher publisher = null;
        try {
            //String topicId = System.getenv("PUBSUB_TOPIC");
            String topicId = "fill-message-details";
            // create a publisher on the topic
            if (publisher == null) {
                ProjectTopicName topicName = ProjectTopicName.newBuilder()
                        .setProject(ServiceOptions.getDefaultProjectId())
                        .setTopic(topicId)
                        .build();
                GoogleCredentials credentials = GoogleCredentials.fromStream(
                        new FileInputStream(file));
                publisher = Publisher.newBuilder(topicName).setCredentialsProvider(FixedCredentialsProvider.create(credentials)).build();
            }
            // construct a pubsub message from the payload
            int count = Integer.valueOf(req.getParameter("count"));

            for (int i = 0; i < count; i++) {
                String data = i + "|" + i + 1 + "|" + "test" + i;
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
                    }

                    @Override
                    public void onSuccess(String messageId) {
                        // Once published, returns server-assigned message ids (unique within the topic)
                        System.out.println("onSuccess with messageId:" + messageId + " , data:" + data);
                    }
                });
            }
            // redirect to home page
            resp.sendRedirect("/");
        } catch (Exception e) {
            resp.sendError(HttpStatus.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }


    }


}
