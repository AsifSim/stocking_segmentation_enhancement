/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.example.entrypoint;

import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverse;
import org.apache.flink.statefun.flink.core.StatefulFunctionsUniverseProvider;
import org.apache.flink.statefun.flink.core.spi.Modules;
import org.example.io.binders.egress.v1.Module;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ClassPathUniverseProvider implements StatefulFunctionsUniverseProvider {
  private static final Logger logger = LoggerFactory.getLogger(ClassPathUniverseProvider.class);
  private static final long serialVersionUID = 1;

  @Override
  public StatefulFunctionsUniverse get(
      ClassLoader classLoader, StatefulFunctionsConfig configuration) {
    logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    logger.info("Parameter(classLoader = {}, configuration = {})",classLoader.toString(),configuration.toString());
    Modules modules = Modules.loadFromClassPath(configuration);
    logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
    return modules.createStatefulFunctionsUniverse();
  }
}
