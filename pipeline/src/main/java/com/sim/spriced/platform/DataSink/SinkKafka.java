package com.sim.spriced.platform.DataSink;

import com.sim.spriced.platform.Operations.Serialization.PricePojoSerializationSchema;
import com.sim.spriced.platform.Pojo.PricePojo;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class SinkKafka {

    private static final Logger logger = LoggerFactory.getLogger(SinkKafka.class);

    @Value("${spring.kafka.producer.bootstrap-servers}")
    private static String BOOTSTRAP_SERVER = "localhost:29092";

    public static KafkaSink<PricePojo> getKafkaSink() {
        logger.info("{}.{} - Creating KafkaSink with bootstrap server: {}", SinkKafka.class.getSimpleName(), "getKafkaSink", BOOTSTRAP_SERVER);
        KafkaSink<PricePojo> kafkaSink = KafkaSink.<PricePojo>builder()
                .setBootstrapServers(BOOTSTRAP_SERVER)
                .setRecordSerializer(new PricePojoSerializationSchema("ingress"))
                .build();
        logger.info("{}.{} - KafkaSink created successfully with topic: {}", SinkKafka.class.getSimpleName(), "getKafkaSink", "ingress");
        return kafkaSink;
    }

    public Properties getProducerConfig() {
        logger.info("{}.{} - Creating producer configuration properties", SinkKafka.class.getSimpleName(), "getProducerConfig");
        Properties properties = new Properties();
        properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 900000);
        logger.debug("{}.{} - Added TRANSACTION_TIMEOUT_CONFIG with value: {}", SinkKafka.class.getSimpleName(), "getProducerConfig", 900000);
        return properties;
    }
}
