package org.example;

import org.apache.flink.statefun.flink.io.spi.SinkProvider;
import org.example.PlaygroundEgressSpec;
import org.apache.flink.statefun.sdk.io.EgressSpec;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlaygroundSinkProvider implements SinkProvider {
  private static final Logger logger = LoggerFactory.getLogger(PlaygroundSinkProvider.class);

  @Override
  public <T> SinkFunction<T> forSpec(EgressSpec<T> spec) {
    logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    logger.info("Parameter(spec = {})",spec.toString());
    final PlaygroundEgressSpec egressSpec = asPlaygroundEgressSpec(spec);
    logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
    return new PlaygroundEgress<>(egressSpec.getPort(), egressSpec.getTopics());
  }

  private static <T> PlaygroundEgressSpec asPlaygroundEgressSpec(EgressSpec<T> spec) {
    logger.info("=============Inside Class PlaygroundSinkProvider, Function asPlaygroundEgressSpec");
    logger.info("Parameter(spec = {})",spec.toString());
    System.out.println("=============Inside the PlaygroundEgressSpec of egress");
    if (spec instanceof PlaygroundEgressSpec) {
      logger.info("=============Going out of asPlaygroundEgressSpec");
      return (PlaygroundEgressSpec) spec;
    }
    logger.error("IllegalArgumentException, Unknown egress spec {}", spec);
    throw new IllegalArgumentException(String.format("Unknown egress spec %s.", spec));
  }
}
