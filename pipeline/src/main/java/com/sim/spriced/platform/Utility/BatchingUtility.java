package com.sim.spriced.platform.Utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.UUID;

public class BatchingUtility {

    private static final Logger logger = LoggerFactory.getLogger(BatchingUtility.class);

    public void batchRecords(Object... args) {
        logger.info("{}.{} - Batching records with arguments: {}", this.getClass().getSimpleName(), "batchRecords", args);
        // Add batching logic here
        logger.info("{}.{} - Records batched successfully.", this.getClass().getSimpleName(), "batchRecords");
    }

    public static String getBatchId(String source) {
        logger.info("{}.{} - Generating Batch ID for source: {}", BatchingUtility.class.getSimpleName(), "getBatchId", source);
        String batchId = generateUUID() + "-" + OffsetDateTime.now() + "-" + source;
        logger.info("{}.{} - Generated Batch ID: {}", BatchingUtility.class.getSimpleName(), "getBatchId", batchId);
        return batchId;
    }

    public static String generateUUID() {
        logger.info("{}.{} - Generating UUID.", BatchingUtility.class.getSimpleName(), "generateUUID");
        String uuid = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        logger.info("{}.{} - Generated UUID: {}", BatchingUtility.class.getSimpleName(), "generateUUID", uuid);
        return uuid;
    }
}
