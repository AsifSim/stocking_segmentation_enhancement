package com.sim.spriced.platform.Pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.sim.spriced.platform.Constants.Constants;
import com.sim.spriced.platform.Pojo.PricePojo;
import com.sim.spriced.platform.Utility.BatchingUtility;
import lombok.SneakyThrows;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.wildfly.common.function.ExceptionPredicate;

import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.DecimalFormat;
import java.util.*;

@Service
public class PipelineService {

    private static final Logger logger = LoggerFactory.getLogger(PipelineService.class);
    private final KafkaTemplate<String, String> kafkaTemplate;

    private static final String CSV_OUTPUT_PATH = "C:\\sim\\outbound\\outbound-data.csv";

    public PipelineService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        logger.info("{}.{} - Initialized with KafkaTemplate.", this.getClass().getSimpleName(), "PipelineService");
    }

    @SneakyThrows
    public void loadFileMetadata(Path path) {
        logger.info("{}.{} - Loading file metadata for path: {}", this.getClass().getSimpleName(), "loadFileMetadata", path);

        BasicFileAttributes basicFileAttributes = Files.readAttributes(path, BasicFileAttributes.class);

        logFileAttributes(basicFileAttributes);
    }

    private void logFileAttributes(BasicFileAttributes basicFileAttributes) {
        logger.debug("{}.{} - File creation time: {}", this.getClass().getSimpleName(), "logFileAttributes", basicFileAttributes.creationTime());
        logger.debug("{}.{} - File last access time: {}", this.getClass().getSimpleName(), "logFileAttributes", basicFileAttributes.lastAccessTime());
        logger.debug("{}.{} - File last modified time: {}", this.getClass().getSimpleName(), "logFileAttributes", basicFileAttributes.lastModifiedTime());
        logger.debug("{}.{} - Is regular file: {}", this.getClass().getSimpleName(), "logFileAttributes", basicFileAttributes.isRegularFile());
    }

    public void csvToKafka(String path, String topic)
    {
        try(CSVReader csvReader = new CSVReader(new FileReader(path))) {
            String[] nextRecord;
            Random random = new Random();
            int i = 0;
            while((nextRecord = csvReader.readNext()) != null)
            {
                String key = Integer.toString(random.nextInt(100) + 1);
                String value = getJson(key,formatData(Double.valueOf(nextRecord[1])));
                System.out.println(value);
                kafkaTemplate.send(topic,key,value);
                i++;
                if(i == 300)
                    break;
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
    public void csvToKafka(String path, String topic, int batch)
    {
        try(CSVReader csvReader = new CSVReader(new FileReader(path))) {
            String[] nextRecord;
            Random random = new Random();
            int i = 0;
            while((nextRecord = csvReader.readNext()) != null)
            {
                String key = Integer.toString(random.nextInt(100) + 1);
                String value = getJson(key,formatData(Double.valueOf(nextRecord[1])));
                System.out.println(value);
                kafkaTemplate.send(topic,key,value);
                i++;
                if(i == 300)
                    break;
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
    public void csvToKafka(String path, String topic, String batchKey)
    {
        try(CSVReader csvReader = new CSVReader(new FileReader(path))) {
            csvReader.skip(1);
            String[] nextRecord;
            String prevKey = "";
            Map<String,Object> mp = new HashMap<>();
            int i =0;
            while((nextRecord = csvReader.readNext()) != null)
            {
              //  String key = nextRecord[0];
//                   System.out.println("The current key is " + key);
//                    System.out.println("The previous key is " + prevKey);
                    if(!mp.isEmpty())
                    {
                        Map<String,Object> dataToBeSent =getBatchData(mp, Constants.NRP,"part");
                        System.out.println(dataToBeSent.toString());
                        Gson gson = new Gson();
                        kafkaTemplate.send("user-logins","testKey",gson.toJson(dataToBeSent));
                        mp = new HashMap<>();
                    }
                mp.put("part_number",nextRecord[0]);
                mp.put("price",nextRecord[1]);
               // mp.put("row"+String.valueOf(i),temp);
              //  kafkaTemplate.send(topic,key,value);
                i++;
                if(i == 10)
                    break;
            }
//            if(!mp.isEmpty())
//            {
//                Map<String,Object> dataToBeSent =getBatchData(mp, Constants.NRP,"part");
//                System.out.println(dataToBeSent.toString());
//                Gson gson = new Gson();
//                kafkaTemplate.send("user-logins",dataToBeSent.get("batch_id").toString(),gson.toJson(dataToBeSent));
//                mp = new HashMap<>();
//                i = 0;
//            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    private Map<String,Object> getBatchData(Map<String,Object> data, String business, String table)
    {
        Map<String,Object> res = new HashMap<>();
        res.put("data", data);
       // res.put("batch_id", BatchingUtility.getBatchId(business,table));
        return res;
    }

//    @KafkaListener(topics = "greetings", groupId = "test")
//    public void listen(String message)
//    {
//        System.out.println("The message from kafka topic is " + message);
//        csvSink(message);
//    }

    private void csvSink(String message)
    {
        try(CSVWriter writer = new CSVWriter(new FileWriter(CSV_OUTPUT_PATH,true)))
        {
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String,String> data = objectMapper.readValue(message,Map.class);
            String[] fields = {data.get("part_number"),data.get("price")};
            writer.writeNext(fields);
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    private String getJson(Object... args) {
        logger.info("{}.{} - Creating JSON with part_number: {} and price: {}", this.getClass().getSimpleName(), "getJson", args[0], args[1]);
        String json = String.format("{\"part_number\": \"%s\", \"price\": \"%s\"}", args[0], args[1]);
        logger.debug("{}.{} - Generated JSON: {}", this.getClass().getSimpleName(), "getJson", json);
        return json;
    }

    private Double formatData(Double a) {
        logger.info("{}.{} - Formatting data value: {}", this.getClass().getSimpleName(), "formatData", a);
        String formatted = new DecimalFormat("#.0000").format(a);
        Double formattedValue = Double.parseDouble(formatted);
        logger.debug("{}.{} - Formatted data value: {}", this.getClass().getSimpleName(), "formatData", formattedValue);
        return formattedValue;
    }

}
