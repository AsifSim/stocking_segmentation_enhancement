package com.sim.spriced.platform.Connectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public interface CustomConnectors<T> {

    Logger logger = LoggerFactory.getLogger(CustomConnectors.class);

    default void initConnection(Properties properties) {
        logger.info("Executing {}.{} with properties: {}",
                this.getClass().getSimpleName(),
                "initConnection",
                properties);
        try {
            // Simulate implementation
            logger.debug("{}.{} executed successfully with properties: {}",
                    this.getClass().getSimpleName(),
                    "initConnection",
                    properties);
        } catch (Exception e) {
            logger.error("Error in {}.{}: {}",
                    this.getClass().getSimpleName(),
                    "initConnection",
                    e.getMessage(),
                    e);
            throw new RuntimeException("Initialization failed.", e);
        }
    }

    default void closeConnection() {
        logger.info("Executing {}.{}",
                this.getClass().getSimpleName(),
                "closeConnection");
        try {
            // Simulate implementation
            logger.debug("{}.{} executed successfully.",
                    this.getClass().getSimpleName(),
                    "closeConnection");
        } catch (Exception e) {
            logger.error("Error in {}.{}: {}",
                    this.getClass().getSimpleName(),
                    "closeConnection",
                    e.getMessage(),
                    e);
            throw new RuntimeException("Closure failed.", e);
        }
    }
}
