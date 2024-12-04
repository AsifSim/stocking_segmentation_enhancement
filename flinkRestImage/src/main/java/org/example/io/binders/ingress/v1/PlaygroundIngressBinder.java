package org.example.io.binders.ingress.v1;

import org.apache.flink.statefun.extensions.ComponentBinder;
import org.apache.flink.statefun.extensions.ComponentJsonObject;
import org.apache.flink.statefun.flink.io.common.AutoRoutableProtobufRouter;
import org.example.io.binders.Utils;
import org.example.PlaygroundIngressSpec;
import org.apache.flink.statefun.sdk.TypeName;
import org.apache.flink.statefun.sdk.spi.StatefulFunctionModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlaygroundIngressBinder implements ComponentBinder {
  private static final Logger logger = LoggerFactory.getLogger(PlaygroundIngressBinder.class);

  static final PlaygroundIngressBinder INSTANCE = new PlaygroundIngressBinder();
  static final TypeName KIND_TYPE = TypeName.parseFrom("io.statefun.playground.v1/ingress");

  @Override
  public void bind(
      ComponentJsonObject component, StatefulFunctionModule.Binder remoteModuleBinder) {
    logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    logger.info("Parameter(component = {}, remoteModuleBinder = {})",component.toString(),remoteModuleBinder.toString());
    Utils.validateComponent(component, KIND_TYPE);
    final PlaygroundIngressSpec playgroundIngressSpec =
        Utils.parseJson(component.specJsonNode(), PlaygroundIngressSpec.class);
    remoteModuleBinder.bindIngress(playgroundIngressSpec);
    remoteModuleBinder.bindIngressRouter(
        playgroundIngressSpec.id(), new AutoRoutableProtobufRouter());
    logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
  }
}
