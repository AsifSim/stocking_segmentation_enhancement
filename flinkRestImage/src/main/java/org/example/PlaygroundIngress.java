package org.example;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.statefun.flink.io.generated.AutoRoutable;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.example.IngressWebServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PlaygroundIngress<T> extends RichSourceFunction<T> {
  private static final Logger logger = LoggerFactory.getLogger(PlaygroundIngress.class);

  private final int port;
  private final BlockingQueue<AutoRoutable> messageQueue;
  private transient IngressWebServer server;

  private volatile boolean running = true;

  PlaygroundIngress(int port) {
    this.port = port;
    this.messageQueue = new ArrayBlockingQueue<>(1 << 20);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    logger.info("parameters(port = {})",parameters.toString());
    super.open(parameters);
    this.server = new IngressWebServer(port, messageQueue);
    logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
  }

  @Override
  public void run(SourceContext<T> sourceContext) throws Exception {
    logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    logger.info("parameters(sourceContext = {})",sourceContext.toString());
    while (running) {
      final AutoRoutable message = messageQueue.poll(50L, TimeUnit.MILLISECONDS);
      if (message != null) {
        synchronized (sourceContext.getCheckpointLock()) {
          sourceContext.collect((T) message);}}}
    if (server != null) server.stop();
    logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
  }

  @Override
  public void cancel() {
    logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
    running = false;
  }
}
