package com.adara.hackathon.sub;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class BQWriter {
    BigQuery bigQuery = null;

    String projectId = null;
    TableId tableId = null;

    private static final Pattern pipeSpliter = Pattern.compile("\\|");

    public  BQWriter() {
        bigQuery = getGoogleBigQueryInstance();
        String datasetName = "spore_drive";
        String tableName = "cookie_key_value";
        tableId = TableId.of(datasetName, tableName);
    }

    public void streamDataToBQ(String rowString) {
        String[] rowArray = pipeSpliter.split(rowString);
        Integer cookieId = Integer.valueOf(rowArray[0]);
        String key = rowArray[1];
        String value = rowArray[2];

        Map<String, Object> rowContent = new HashMap<>();
        rowContent.put("cookie_id", cookieId);
        rowContent.put("key_id", key);
        rowContent.put("value", value);

       InsertAllResponse response =  bigQuery.insertAll( InsertAllRequest.newBuilder(tableId).addRow(rowContent).build());
                                // More rows can be added in the same RPC by invoking .addRow() on the builder
        if (response.hasErrors()) {
            // If any of the insertions failed, this lets you inspect the errors
            for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                //System.out.println("[BQWriter.streamDataToBQ] response.hasErrors:" + entry);
                // inspect row error
            }
        }

    }


    /**
     *
     * @return
     * @throws Exception
     */
    private BigQuery getGoogleBigQueryInstance() {

        GoogleCredentials credentials = null;
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("adara-spore-drive-7a12bb7e0cfd.json").getFile());
        try {
            credentials = GoogleCredentials.fromStream(new FileInputStream(file));
        }catch (Exception e){

        }

        // Build service account credential.
       // Credential credential = getServiceAccountCredential(httpTransport, jsonFactory, serviceAccountEmail, p12File);

        // Create DFA Reporting client.
        BigQuery bigquery = BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
        return bigquery;
    }


}
