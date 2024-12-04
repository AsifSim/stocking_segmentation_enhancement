package org.example;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

public class EgressRecord {
    @JsonProperty("topic")
    private String topic;

    @JsonProperty("payload")
    private String payload;

    @JsonProperty("key")
    private String key;

    public String getKey() {
        return key;
    }

    public EgressRecord() {
        this(null, null,null);
    }

    public EgressRecord(String topic, String payload,String key) {
        this.topic = topic;
        this.payload = payload;
        this.key=key;
    }

    public String getTopic() {
        return topic;
    }

    public String getPayload() {
        return payload;
    }
}

