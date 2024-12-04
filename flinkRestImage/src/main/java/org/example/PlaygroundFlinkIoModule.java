package org.example;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.flink.statefun.flink.io.spi.FlinkIoModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(FlinkIoModule.class)
public class PlaygroundFlinkIoModule implements FlinkIoModule {
  private static final Logger logger = LoggerFactory.getLogger(PlaygroundFlinkIoModule.class);
  @Override
  public void configure(Map<String, String> globalConfiguration, Binder binder) {
    logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    logger.info("Parameter(globalConfiguration = {}, binder = {})",globalConfiguration.toString(),binder.toString());
    binder.bindSourceProvider(Constants.INGRESS_TYPE, new PlaygroundSourceProvider());
    binder.bindSinkProvider(Constants.EGRESS_TYPE, new PlaygroundSinkProvider());
    logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
  }
}
