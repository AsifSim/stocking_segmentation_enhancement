package org.example;

import com.google.protobuf.ByteString;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import io.undertow.util.StatusCodes;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.http.HttpClient;

final class EgressWebServer implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(EgressWebServer.class);
  private final ConcurrentMap<String, BlockingQueue<ByteString>> queues;
  private final Undertow server;

  EgressWebServer(int port, Set<String> topics) {
    logger.info("=============Inside Class EgressWebServer, Function EgressWebServer");
    logger.info("Parameter(port = {}, topics = {})",port,topics.toString());
    this.queues = createQueues(topics);
    this.server = Undertow.builder().addHttpListener(port, "0.0.0.0").setHandler(new EgressHttpHandler(queues)).build();
    server.start();
  }

  private static ConcurrentMap<String, BlockingQueue<ByteString>> createQueues(Set<String> topics) {
    logger.info("=============Inside Class EgressWebServer, Function createQueues");
    logger.info("Parameter(topics = {})",topics.toString());
    logger.info("=============Going out of createQueues");
    return topics.stream().collect(Collectors.toConcurrentMap(Function.identity(), ignored -> new ArrayBlockingQueue<>(1 << 20)));
  }

  @Override
  public void close() {
    server.stop();
  }

  public void offer(String topic, ByteString message, String key) {
    logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    logger.info("Parameter(message = {}, key = {})",message.toString(),key);
    byte[] byteArrayValue = message.toByteArray();
    logger.info("byteArrayValue {}",byteArrayValue);
    sendToEndpoint(key,byteArrayValue);
    logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
  }


  private void sendToEndpoint(String key, byte[] message) {
    logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    logger.info("Parameter(message = {}, key = {})",message.toString(),key);
    try {
      HttpClient client = HttpClient.newHttpClient();
      String jsonPayload = String.format("{\"response\":%s}", new String(message, StandardCharsets.UTF_8));
      System.out.println("json payload inside http egress is "+jsonPayload);
      HttpRequest request = HttpRequest.newBuilder()
              .uri(URI.create("http://host.docker.internal:9092/wrapper/egress"))
              .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
              .build();

      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() == 200) System.out.println("Successfully sent data to endpoint");
      else System.out.println("Failed to send data to endpoint. Status: {}"+ response.statusCode());
    } catch (Exception e) {
      System.out.println("Error while sending data to endpoint: {}"+ e.getMessage());
    }
    logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
  }

  private static final class EgressHttpHandler implements HttpHandler {
    private final Map<String, BlockingQueue<ByteString>> queues;

    private EgressHttpHandler(Map<String, BlockingQueue<ByteString>> queues) {
      System.out.println("============In egress HttpHandler");
      this.queues = queues;
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) {
      System.out.println("=============In egress handleRequest");
      final String topic = exchange.getRelativePath().substring(1);

      final BlockingQueue<ByteString> queue = queues.get(topic);

      if (queue != null) {
        final ByteString message = queue.poll();

        if (message != null) {
          exchange.getResponseHeaders().put(Headers.STATUS, StatusCodes.OK);
          exchange.getResponseSender().send(message.asReadOnlyByteBuffer());
        } else {
          exchange.getResponseHeaders().put(Headers.STATUS, StatusCodes.NOT_FOUND);
        }
      } else {
        exchange.getResponseHeaders().put(Headers.STATUS, StatusCodes.METHOD_NOT_ALLOWED);
      }

      exchange.endExchange();
    }
  }
}
