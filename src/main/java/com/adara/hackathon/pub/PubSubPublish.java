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

package com.adara.hackathon.pub;


import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.ProjectTopicName;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class PubSubPublish {
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource("adara-spore-drive-7a12bb7e0cfd.json").getFile());


    public void runPub() throws IOException{
        ScheduledExecutorService thread = Executors.newSingleThreadScheduledExecutor();
        Publisher publisher = null;

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

        PubTask pubTask = new PubTask(publisher);
        int refreshInterval = 10; // seconds
        thread.scheduleWithFixedDelay(pubTask, refreshInterval, refreshInterval, TimeUnit.SECONDS); //等待refreshInterval后执行mDBRefreshTask，3s后任务结束，再等待2s（间隔时间-消耗时间），如果有空余线程时，再次执行该任务

    }
}
