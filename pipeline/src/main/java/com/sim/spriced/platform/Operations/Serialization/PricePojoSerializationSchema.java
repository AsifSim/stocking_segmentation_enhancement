package com.sim.spriced.platform.Operations.Serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sim.spriced.platform.Constants.Constants;
import com.sim.spriced.platform.Pojo.Data;
import com.sim.spriced.platform.Pojo.PricePojo;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;


public class PricePojoSerializationSchema implements KafkaRecordSerializationSchema<PricePojo> {

    private static final Logger logger = LoggerFactory.getLogger(PricePojoSerializationSchema.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final String topic;

    public PricePojoSerializationSchema(String topic) {
        this.topic = topic;
        logger.info("{}.{} - Initialized with topic: {}", this.getClass().getSimpleName(), "PricePojoSerializationSchema", topic);
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(PricePojo element, KafkaSinkContext context, Long timestamp) {
        logger.info("{}.{} - Serializing PricePojo with BatchID: {}", this.getClass().getSimpleName(), "serialize", element.getBatchID());
        byte[] keyBytes = getKeyBytes(element);
        byte[] valueBytes = getValueBytes(element);
        return createProducerRecord(keyBytes, valueBytes, timestamp);
    }

    private byte[] getKeyBytes(PricePojo element) {
        logger.debug("{}.{} - Generating key bytes for BatchID: {}", this.getClass().getSimpleName(), "getKeyBytes", element.getBatchID());
        return element.getBatchID() != null ? element.getBatchID().getBytes(StandardCharsets.UTF_8) : null;
    }

    private byte[] getValueBytes(PricePojo element) {
        try {
            logger.debug("{}.{} - Serializing value bytes for BatchID: {}", this.getClass().getSimpleName(), "getValueBytes", element.getBatchID());
            return objectMapper.writeValueAsBytes(getValueMap(element));
        } catch (Exception e) {
            logger.error("{}.{} - Failed to serialize value bytes for BatchID: {}", this.getClass().getSimpleName(), "getValueBytes", element.getBatchID(), e);
            throw new RuntimeException("Failed to serialize PricePojo", e);
        }
    }

    private Map<String, Object> getValueMap(PricePojo element) {
        logger.debug("{}.{} - Constructing value map for BatchID: {}", this.getClass().getSimpleName(), "getValueMap", element.getBatchID());
        return Map.of(
                Constants.BATCH_ID, element.getBatchID(),
                Constants.DATA, getDataMap(element)
        );
    }

    private Map<String, Object> getDataMap(PricePojo element) {
        logger.debug("{}.{} - Building data map for BatchID: {}", this.getClass().getSimpleName(), "getDataMap", element.getBatchID());
        Map<String, Object> dataMap = new HashMap<>();
        for (Data temp : element.getData()) {
            dataMap.put(temp.getKey(), temp.getValue());
            logger.debug("{}.{} - Added key-value pair to data map: key={}, value={}", this.getClass().getSimpleName(), "getDataMap", temp.getKey(), temp.getValue());
        }
        return dataMap;
    }

    private ProducerRecord<byte[], byte[]> createProducerRecord(byte[] keyBytes, byte[] valueBytes, Long timestamp) {
        logger.debug("{}.{} - Creating ProducerRecord for topic: {}", this.getClass().getSimpleName(), "createProducerRecord", topic);
        return new ProducerRecord<>(topic, null, timestamp, keyBytes, valueBytes);
    }
}
