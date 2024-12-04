package org.example;

import com.google.protobuf.ByteString;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.HeaderValues;
import io.undertow.util.Headers;
import io.undertow.util.StatusCodes;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import org.apache.flink.statefun.flink.io.generated.AutoRoutable;
import org.apache.flink.statefun.flink.io.generated.RoutingConfig;
import org.apache.flink.statefun.flink.io.generated.TargetFunctionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IngressWebServer {
  private static final Logger logger = LoggerFactory.getLogger(IngressWebServer.class);
  private final Undertow server;

  IngressWebServer(int port, BlockingQueue<AutoRoutable> messageQueue) {
    logger.info("=============Inside Class IngressWebServer, Function IngressWebServer");
    logger.info("Parameter(port = {}, messageQueue = {})",port,messageQueue.toString());
    this.server = Undertow.builder().addHttpListener(port, "0.0.0.0").setHandler(new IngressHttpHandler(messageQueue)).build();
    server.start();
  }

  void stop() {
    logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    server.stop();
    logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
  }

  private static final class IngressHttpHandler implements HttpHandler {
    private final BlockingQueue<AutoRoutable> messageQueue;

    private IngressHttpHandler(BlockingQueue<AutoRoutable> messageQueue) {
      logger.info("=============Inside Class IngressHttpHandler, Function IngressHttpHandler");
      logger.info("Parameter(messageQueue = {})",messageQueue.toString());
      this.messageQueue = messageQueue;
      logger.info("=============Going out of IngressHttpHandler");
    }

    @Override
    public void handleRequest(HttpServerExchange httpServerExchange) {
      logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
      logger.info("Parameter(httpServerExchange = {})",httpServerExchange.toString());
      httpServerExchange.getRequestReceiver().receiveFullBytes(this::onRequestBody);
      logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
    }

    private void onRequestBody(HttpServerExchange exchange, byte[] payload) {
      logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
      logger.info("Parameter(exchange = {}, payload = {})",exchange.toString(),payload.toString());
      exchange.dispatch();
      try {
        final Address address = parseAddress(exchange.getRelativePath());
        final String typeUrl = parseTypeUrl(exchange.getRequestHeaders());
        final RoutingConfig routingConfig = createRoutingConfig(address, typeUrl);
        final AutoRoutable autoRoutable = createAutoRoutable(payload, address, routingConfig);
        messageQueue.put(autoRoutable);
        exchange.getResponseHeaders().put(Headers.STATUS, StatusCodes.OK);
      } catch (ParseException | InterruptedException e) {
        e.printStackTrace(System.out);
        exchange.getResponseHeaders().put(Headers.STATUS, StatusCodes.INTERNAL_SERVER_ERROR);
      }

      exchange.endExchange();
      logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
    }

    private AutoRoutable createAutoRoutable(
        byte[] payload, Address address, RoutingConfig routingConfig) {
      logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
      logger.info("Parameter(address = {}, payload = {}, routingConfig = {})",address.toString(),payload.toString(),routingConfig.toString());
      logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
      return AutoRoutable.newBuilder()
          .setId(address.key).setConfig(routingConfig).setPayloadBytes(ByteString.copyFrom(payload)).build();
    }

    private RoutingConfig createRoutingConfig(Address address, String typeUrl) {
      logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
      logger.info("Parameter(address = {}, typeUrl = {})",address.toString(),typeUrl);
      logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
      return RoutingConfig.newBuilder()
          .setTypeUrl(typeUrl).addAllTargetFunctionTypes(Collections.singleton(TargetFunctionType.newBuilder().setNamespace(address.namespace).setType(address.functionType).build()))
          .build();
    }

    private String parseTypeUrl(HeaderMap requestHeaders) {
      logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
      logger.info("Parameter(requestHeaders = {})",requestHeaders.toString());
      final HeaderValues headerValues = requestHeaders.get(Headers.CONTENT_TYPE);
      logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
      return headerValues.stream().filter(value -> value.startsWith(Constants.STATEFUN_CONTENT_TYPE_PREFIX)).findFirst().map(type -> type.substring(Constants.STATEFUN_CONTENT_TYPE_PREFIX.length())).orElse(Constants.DEFAULT_INGRESS_TYPE);
    }

    private Address parseAddress(String relativePath) throws ParseException {
      logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
      logger.info("Parameter(relativePath = {})",relativePath);
      final String[] split = relativePath.substring(1).split("/");
      if (split.length != 3) throw new ParseException("Invalid URL. Please specify '/namespace/function_type/function_id");
      logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
      return new Address(split[0], split[1], split[2]);
    }
  }

  private static final class Address {
    final String namespace;
    final String functionType;
    final String key;

    private Address(String namespace, String functionType, String key) {
      this.namespace = namespace;
      this.functionType = functionType;
      this.key = key;
    }
  }
}
