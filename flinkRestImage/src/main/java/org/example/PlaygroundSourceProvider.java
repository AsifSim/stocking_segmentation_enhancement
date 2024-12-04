package org.example;

import org.apache.flink.statefun.flink.io.spi.SourceProvider;
import org.example.PlaygroundIngressSpec;
import org.apache.flink.statefun.sdk.io.IngressSpec;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.PlaygroundIngress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlaygroundSourceProvider implements SourceProvider {
  private static final Logger logger = LoggerFactory.getLogger(PlaygroundSourceProvider.class);

  @Override
  public <T> SourceFunction<T> forSpec(IngressSpec<T> spec) {
    logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    logger.info("Parameter(spec = {})",spec.toString());
    final PlaygroundIngressSpec ingressSpec = asPlaygroundIngressSpec(spec);
    logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
    return new PlaygroundIngress<T>(ingressSpec.getPort());
  }

  private static <T> PlaygroundIngressSpec asPlaygroundIngressSpec(IngressSpec<T> spec) {
    logger.info("=============Inside Class PlaygroundSourceProvider, Function PlaygroundIngressSpec");
    logger.info("Parameter(spec = {})",spec.toString());
    if (spec instanceof PlaygroundIngressSpec) {
      logger.info("=============Going out of PlaygroundIngressSpec");
      return (PlaygroundIngressSpec) spec;
    }
    logger.error("IllegalArgumentException, Unknown ingress spec {}", spec);
    throw new IllegalArgumentException(String.format("Unknown ingress spec %s", spec));
  }
}
