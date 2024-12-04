package org.example;

import com.google.protobuf.Message;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.flink.statefun.sdk.IngressType;
import org.apache.flink.statefun.sdk.io.IngressIdentifier;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonDeserialize(builder = PlaygroundIngressSpec.Builder.class)
public class PlaygroundIngressSpec implements IngressSpec<Message> {

  private final int port;
  private PlaygroundIngressSpec(int port) {
    this.port = port;
  }
  public int getPort() {
    return port;
  }

  @Override
  public IngressIdentifier<Message> id() {
    return org.example.Constants.INGRESS_IDENTIFIER;
  }

  @Override
  public IngressType type() {
    return org.example.Constants.INGRESS_TYPE;
  }

  @JsonPOJOBuilder
  public static final class Builder {
    private static final Logger logger = LoggerFactory.getLogger(Builder.class);
    private final int port;

    @JsonCreator
    private Builder(@JsonProperty("port") int port) {
      this.port = port;
    }

    public PlaygroundIngressSpec build() {
      logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
      logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
      return new PlaygroundIngressSpec(port);
    }
  }
}
