package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.flink.configuration.Configuration;
import org.example.Constants;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PlaygroundEgress<T> extends RichSinkFunction<T> {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final RefCountedContainer<EgressWebServer> container = new RefCountedContainer<>();

  private final int port;
  private final Set<String> topics;

  private transient RefCountedContainer<EgressWebServer>.Lease handle;
  private static final Logger logger = LoggerFactory.getLogger(RefCountedContainer.class);
  PlaygroundEgress(int port, Set<String> topics) {
    this.port = port;
    this.topics = new HashSet<>(topics);
    this.handle = null;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.handle = container.getOrCreate(() -> new EgressWebServer(port, topics));
  }

  @Override
  public void invoke(T value, Context context) throws Exception {
    System.out.println("===========Inside the invoke of egress");
    final EgressRecord egressRecord = asEgressRecord(value);

    final String topic = egressRecord.getTopic();
    System.out.println("=========The topic is "+topic);
    if (!topics.contains(topic)) {
      System.out.println("===========Inside the invoke of egress in if block");
      throw new IllegalArgumentException(
          String.format("Message was targeted to unknown topic %s.", topic));
    }

    handle.deref().offer(topic, ByteString.copyFromUtf8(egressRecord.getPayload()), egressRecord.getKey());
  }

  private EgressRecord asEgressRecord(T value) throws IOException {
    System.out.println("===========Inside the asEgressRecord of egress");
    if (value instanceof TypedValue) {
      final TypedValue typedValue = ((TypedValue) value);
      System.out.println("===========Inside the asEgressRecord of egress in if block");
      if (typedValue.getTypename().equals(Constants.PLAYGROUND_EGRESS_RECORD)) {
        System.out.println("===========Inside the asEgressRecord of egress in the nested if");
        return objectMapper.readValue(typedValue.getValue().toByteArray(), EgressRecord.class);
      }
    }
    throw new IllegalArgumentException(String.format("Received unexpected value %s.", value));
  }

  @Override
  public void finish() throws Exception {
    System.out.println("===========Inside the finish of egress");
    super.finish();
    handle.close();
  }
}
