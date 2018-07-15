package com.adara.hackathon.sub;

import com.google.cloud.bigquery.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class BQWriter {
    BigQuery bigQuery = null;

    String projectId = null;
    TableId tableId = null;

    private static final Pattern pipeSpliter = Pattern.compile("|");

    public void BQWriter() {
        bigQuery = BigQueryOptions.getDefaultInstance().getService();
        String datasetName = "spore_drive";
        String tableName = "cookie_key_value";
        tableId = TableId.of(datasetName, tableName);
    }

    public void streamDataToBQ(String rowString) {
        String[] rowArray = pipeSpliter.split(rowString);
        Long cookieId = Long.valueOf(rowArray[0]);
        String key = rowArray[1];
        String value = rowArray[2];

        Map<String, Object> rowContent = new HashMap<>();
        rowContent.put("cookie_id", cookieId);
        rowContent.put("key", key);
        rowContent.put("value", value);

        InsertAllResponse response =
                bigQuery.insertAll(
                        InsertAllRequest.newBuilder(tableId)
                                .addRow(rowContent)
                                // More rows can be added in the same RPC by invoking .addRow() on the builder
                                .build());
        if (response.hasErrors()) {
            // If any of the insertions failed, this lets you inspect the errors
            for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                // inspect row error
            }
        }

    }
}
