package com.sim.spriced.platform;

import com.sim.spriced.platform.Connectors.SFTPConnector;
import com.sim.spriced.platform.Constants.Constants;
import com.sim.spriced.platform.DataSink.CSVSinkWriter;
import com.sim.spriced.platform.DataSink.SinkKafka;
import com.sim.spriced.platform.DataSources.SFTPSource;
import com.sim.spriced.platform.DataSources.SourceKafka;
import com.sim.spriced.platform.Operations.JSONCreator;
import com.sim.spriced.platform.Pipeline.PipelineService;
import lombok.SneakyThrows;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import com.sim.spriced.platform.Pojo.PricePojo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;


import java.io.File;
import java.io.IOException;
import java.util.*;

@Component
public class DataStreamService {

    private static final String CONFIG_FILE_PATH = "C:/Work/spriced-platform/pipeline/src/main/resources/config.json";
    private static final Logger logger = LoggerFactory.getLogger(DataStreamService.class);

    @Autowired
    private PipelineService pipelineService;

    @SneakyThrows
    @EventListener(ApplicationReadyEvent.class)
    public void executeJobs() {
        logger.info("{}.{} - Starting job execution.", this.getClass().getSimpleName(), "executeJobs");
        Properties properties = getConfigData();
        logger.debug("{}.{} - Retrieved configuration properties: {}", this.getClass().getSimpleName(), "executeJobs", properties);
        startStream(properties);
        logger.info("{}.{} - Job execution completed.", this.getClass().getSimpleName(), "executeJobs");
    }

    public void startStream(Properties properties) throws Exception {
        logger.info("{}.{} - Starting the stream job.", this.getClass().getSimpleName(), "startStream");
        StreamExecutionEnvironment env = configureEnvironment();
        setupSFTPToKafkaStream(env, properties);
        setupKafkaToFileStream(env, properties);
        env.execute("SFTP JOB");
        logger.info("{}.{} - Stream job completed successfully.", this.getClass().getSimpleName(), "startStream");
    }

    private StreamExecutionEnvironment configureEnvironment() {
        logger.debug("{}.{} - Configuring StreamExecutionEnvironment.", this.getClass().getSimpleName(), "configureEnvironment");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.enableCheckpointing(60000);
        env.setParallelism(1);
        return env;
    }

    private void setupSFTPToKafkaStream(StreamExecutionEnvironment env, Properties properties) {
        logger.info("{}.{} - Setting up SFTP to Kafka stream.", this.getClass().getSimpleName(), "setupSFTPToKafkaStream");
        Source<PricePojo, ?, ?> source = new SFTPSource(properties);
        KafkaSink<PricePojo> kafkaSink = SinkKafka.getKafkaSink();
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "SFTP CSV Source")
                .keyBy(PricePojo::getBatchID)
                .map(new JSONCreator())
                .sinkTo(kafkaSink);
        logger.debug("{}.{} - SFTP to Kafka stream setup completed.", this.getClass().getSimpleName(), "setupSFTPToKafkaStream");
    }

    private void setupKafkaToFileStream(StreamExecutionEnvironment env, Properties properties) {
        logger.info("{}.{} - Setting up Kafka to File stream.", this.getClass().getSimpleName(), "setupKafkaToFileStream");
        KafkaSource<String> kafkaSource = SourceKafka.getKafkaSource();
        FileSink<String> fileSink = createFileSink(properties);
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KAFKA Source")
                .map(this::processKafkaRecord)
                .sinkTo(fileSink);
        logger.debug("{}.{} - Kafka to File stream setup completed.", this.getClass().getSimpleName(), "setupKafkaToFileStream");
    }

    private FileSink<String> createFileSink(Properties properties) {
        logger.debug("{}.{} - Creating FileSink with path: {}", this.getClass().getSimpleName(), "createFileSink", properties.getProperty("sinkPath"));
        OutputFileConfig config = OutputFileConfig.builder()
                .withPartPrefix("test-1")
                .withPartSuffix(".csv")
                .build();
        return CSVSinkWriter.getFileSink(properties.getProperty("sinkPath"), config);
    }

    private String processKafkaRecord(String s) throws Exception {
        logger.debug("{}.{} - Processing Kafka record.", this.getClass().getSimpleName(), "processKafkaRecord");
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(s);
        JsonNode data = rootNode.get("data");
        Map<String, Object> dataMap = objectMapper.treeToValue(data, Map.class);
        return joinDataMap(dataMap);
    }

    private String joinDataMap(Map<String, Object> dataMap) {
        logger.debug("{}.{} - Joining data map.", this.getClass().getSimpleName(), "joinDataMap");
        StringBuilder temp = new StringBuilder();
        dataMap.forEach((key, value) -> temp.append(value).append(","));
        temp.deleteCharAt(temp.length() - 1);
        return temp.toString();
    }


    @SneakyThrows
    public Properties getConfigData() {
        logger.info("{}.{} - Loading configuration data.", this.getClass().getSimpleName(), "getConfigData");

        Properties properties = new Properties();
        JsonNode jsonObject = loadConfigFile().get("sftp");

        properties.setProperty(SFTPConnector.USERNAME, jsonObject.get("username").asText());
        logger.debug("{}.{} - Loaded property: {}={}", this.getClass().getSimpleName(), "getConfigData", SFTPConnector.USERNAME, jsonObject.get("username").asText());

        properties.setProperty(SFTPConnector.HOST, jsonObject.get("hostname").asText());
        logger.debug("{}.{} - Loaded property: {}={}", this.getClass().getSimpleName(), "getConfigData", SFTPConnector.HOST, jsonObject.get("hostname").asText());

        properties.setProperty(SFTPConnector.PORT, jsonObject.get("port").asText());
        logger.debug("{}.{} - Loaded property: {}={}", this.getClass().getSimpleName(), "getConfigData", SFTPConnector.PORT, jsonObject.get("port").asText());

        properties.setProperty(SFTPConnector.SOURCE, jsonObject.get("remotePath").asText());
        logger.debug("{}.{} - Loaded property: {}={}", this.getClass().getSimpleName(), "getConfigData", SFTPConnector.SOURCE, jsonObject.get("remotePath").asText());

        properties.setProperty(SFTPConnector.DESTINATION, jsonObject.get("destination").asText());
        logger.debug("{}.{} - Loaded property: {}={}", this.getClass().getSimpleName(), "getConfigData", SFTPConnector.DESTINATION, jsonObject.get("destination").asText());

        properties.setProperty(Constants.BATCHING, jsonObject.get(Constants.BATCHING).asText());
        logger.debug("{}.{} - Loaded property: {}={}", this.getClass().getSimpleName(), "getConfigData", Constants.BATCHING, jsonObject.get(Constants.BATCHING).asText());

        properties.setProperty(Constants.BATCH_KEY, jsonObject.get(Constants.BATCH_KEY).asText());
        logger.debug("{}.{} - Loaded property: {}={}", this.getClass().getSimpleName(), "getConfigData", Constants.BATCH_KEY, jsonObject.get(Constants.BATCH_KEY).asText());

        properties.setProperty(Constants.TABLE, jsonObject.get(Constants.TABLE).asText());
        logger.debug("{}.{} - Loaded property: {}={}", this.getClass().getSimpleName(), "getConfigData", Constants.TABLE, jsonObject.get(Constants.TABLE).asText());

        properties.setProperty(Constants.FILE_NAME, jsonObject.get(Constants.FILE_NAME).asText());
        logger.debug("{}.{} - Loaded property: {}={}", this.getClass().getSimpleName(), "getConfigData", Constants.FILE_NAME, jsonObject.get(Constants.FILE_NAME).asText());

        properties.setProperty(Constants.SINK_PATH, jsonObject.get(Constants.SINK_PATH).asText());
        logger.debug("{}.{} - Loaded property: {}={}", this.getClass().getSimpleName(), "getConfigData", Constants.SINK_PATH, jsonObject.get(Constants.SINK_PATH).asText());

        logger.info("{}.{} - Configuration data loaded successfully.", this.getClass().getSimpleName(), "getConfigData");
        return properties;
    }


    private JsonNode loadConfigFile() throws IOException {
        logger.info("{}.{} - Loading configuration file from path: {}", this.getClass().getSimpleName(), "loadConfigFile", CONFIG_FILE_PATH);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode configNode;
        try {
            configNode = objectMapper.readTree(new File(CONFIG_FILE_PATH));
            logger.info("{}.{} - Configuration file loaded successfully.", this.getClass().getSimpleName(), "loadConfigFile");
        } catch (IOException e) {
            logger.error("{}.{} - Failed to load configuration file: {}", this.getClass().getSimpleName(), "loadConfigFile", e.getMessage(), e);
            throw e;
        }
        return configNode;
    }


}