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

  public EgressRecord() {}

  public String getTopic() {
    return topic;
  }

  public String getPayload() {
    return payload;
  }
}
