package org.example.io.binders.ingress.v1;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.flink.statefun.extensions.ExtensionModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(ExtensionModule.class)
public final class Module implements ExtensionModule {
  private static final Logger logger = LoggerFactory.getLogger(Module.class);

  @Override
  public void configure(Map<String, String> globalConfigurations, Binder binder) {
    logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    logger.info("Parameter(globalConfigurations {}, binder = {})",globalConfigurations.toString(),binder.toString());
    binder.bindExtension(PlaygroundIngressBinder.KIND_TYPE, PlaygroundIngressBinder.INSTANCE);
    logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
  }
}
