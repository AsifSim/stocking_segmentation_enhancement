package com.sim.spriced.platform.DataSources;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceKafka {

    private static final Logger logger = LoggerFactory.getLogger(SourceKafka.class);

    public static KafkaSource<String> getKafkaSource() {
        logger.info("Executing {}.getKafkaSource - Creating KafkaSource with bootstrap servers: {}, topic: {}, and group ID: {}",
                SourceKafka.class.getSimpleName(), "localhost:29092", "egress", "my-group-id");

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:29092")
                .setTopics("egress")
                .setGroupId("my-group-id")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        logger.info("{}.getKafkaSource - KafkaSource created successfully with topic: {}",
                SourceKafka.class.getSimpleName(), "egress");

        return kafkaSource;
    }
}
