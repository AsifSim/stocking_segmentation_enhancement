package org.example;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.handler.RequestReplyHandler;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.Slices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class UndertowHttpHandler implements HttpHandler {
    private final RequestReplyHandler handler;
//    private static final Logger logger = LoggerFactory.getLogger(UndertowHttpHandler.class);

    public UndertowHttpHandler(RequestReplyHandler handler) {
        System.out.println("=========Inside UndertowHttpHandler");
//        logger.info("=============Inside Class UndertowHttpHandler, Function UndertowHttpHandler");
//        logger.info("Parameter(handler = {})",handler.toString());
        this.handler = Objects.requireNonNull(handler);
//        logger.info("=============Going out of UndertowHttpHandler");
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) {
        System.out.println("=========Inside handleRequest");
//        logger.info("=============Inside Class UndertowHttpHandler, Function handleRequest");
//        logger.info("Parameter(exchange = {})",exchange.toString());
        exchange.getRequestReceiver().receiveFullBytes(this::onRequestBody);
//        logger.info("=============Going out of handleRequest");
    }

    private void onRequestBody(HttpServerExchange exchange, byte[] requestBytes) {
        System.out.println("=========Inside onRequestBody");
//        logger.info("=============Inside Class UndertowHttpHandler, Function onRequestBody");
//        logger.info("Parameter(exchange = {}, requestBytes = {})",exchange.toString(),requestBytes.toString());
        exchange.dispatch();
        CompletableFuture<Slice> future = handler.handle(Slices.wrap(requestBytes));
        future.whenComplete((response, exception) -> onComplete(exchange, response, exception));
//        logger.info("=============Going out of onRequestBody");
    }

    private void onComplete(HttpServerExchange exchange, Slice responseBytes, Throwable ex) {
        System.out.println("=========Inside onComplete");
//        logger.info("=============Inside Class UndertowHttpHandler, Function onComplete");
//        logger.info("Parameter(exchange = {}, responseBytes = {}, ex = {})",exchange.toString(),responseBytes.toString(),ex.toString());
        if (ex != null) {
            ex.printStackTrace(System.out);
            exchange.getResponseHeaders().put(Headers.STATUS, 500);
            exchange.endExchange();
//            logger.info("=============Going out of onComplete");
            return;
        }
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/octet-stream");
        exchange.getResponseSender().send(responseBytes.asReadOnlyByteBuffer());
//        logger.info("=============Going out of onComplete");
    }
}

