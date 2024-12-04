package org.example.io.binders.egress.v1;

import org.apache.flink.statefun.extensions.ComponentBinder;
import org.apache.flink.statefun.extensions.ComponentJsonObject;
import org.example.io.binders.Utils;
import org.example.PlaygroundEgressSpec;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlaygroundEgressBinder implements ComponentBinder {
  private static final Logger logger = LoggerFactory.getLogger(PlaygroundEgressBinder.class);

  static final PlaygroundEgressBinder INSTANCE = new PlaygroundEgressBinder();
  static final TypeName KIND_TYPE = TypeName.parseFrom("io.statefun.playground.v1/egress");

  @Override
  public void bind(
      ComponentJsonObject component, StatefulFunctionModule.Binder remoteModuleBinder) {
    logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    logger.info("Parameter(component = {}, remoteModuleBinder = {})",component.toString(),remoteModuleBinder.toString());
    Utils.validateComponent(component, KIND_TYPE);
    logger.info("playgroundEgress = {}",PlaygroundEgressSpec.class);
    final PlaygroundEgressSpec playgroundEgressSpec = Utils.parseJson(component.specJsonNode(), PlaygroundEgressSpec.class);
    remoteModuleBinder.bindEgress(playgroundEgressSpec);
    logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
  }
}
