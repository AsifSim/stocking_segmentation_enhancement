package com.sim.spriced.platform.Utility;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sim.spriced.platform.DataSources.SourceKafka;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

@Service
public class FileCheck {
    private static RedisTemplate<String, String> redisTemplate = new RedisTemplate<>();

    private static final Logger logger = LoggerFactory.getLogger(FileCheck.class);

    @Autowired
    public FileCheck(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }


    //Call this static function to check if the file is a duplicate or not.
    public static boolean checkDuplicate(String fileName) {
        logger.info("{}.{} - Checking for duplicate fileName: {}", FileCheck.class.getSimpleName(), "checkDuplicate", fileName);

        return Optional.ofNullable(redisTemplate.opsForValue().get(fileName))
                .map(count -> {
                    logger.debug("{}.{} - Duplicate found for fileName: {}", FileCheck.class.getSimpleName(), "checkDuplicate", fileName);
                    return true;
                })
                .orElseGet(() -> {
                    redisTemplate.opsForValue().set(fileName, "1");
                    logger.debug("{}.{} - No duplicate found, fileName added to cache: {}", FileCheck.class.getSimpleName(), "checkDuplicate", fileName);
                    return false;
                });
    }


    //Call this static function to check if the file name is valid or not.
    public static boolean fileNameValidation(String fileName) throws IOException {
        logger.info("{}.{} - Validating fileName: {}", FileCheck.class.getSimpleName(), "fileNameValidation", fileName);

        JsonNode numberOfRecord = getJson("numberOfRecord");
        boolean isNumberOfRecordTrue = numberOfRecord.asText().equalsIgnoreCase("true");
        logger.debug("{}.{} - 'numberOfRecord' JSON property: {}", FileCheck.class.getSimpleName(), "fileNameValidation", isNumberOfRecordTrue);

        String timestampOfFile = extractTimestamp(fileName, isNumberOfRecordTrue);
        String onlyFileName = extractFileName(fileName, isNumberOfRecordTrue);

        logger.debug("{}.{} - Extracted timestamp: {}", FileCheck.class.getSimpleName(), "fileNameValidation", timestampOfFile);
        logger.debug("{}.{} - Extracted only fileName: {}", FileCheck.class.getSimpleName(), "fileNameValidation", onlyFileName);

        return validateFileName(onlyFileName, timestampOfFile);
    }

    private static String extractTimestamp(String fileName, boolean isNumberOfRecordTrue) {
        return isNumberOfRecordTrue
                ? fileName.split("_")[fileName.split("_").length - 2]
                : fileName.split("_")[fileName.split("_").length - 1];
    }

    private static String extractFileName(String fileName, boolean isNumberOfRecordTrue) {
        String regex = isNumberOfRecordTrue
                ? "([a-zA-Z_]+)_\\d{14}_\\d+"
                : "([a-zA-Z_]+)_\\d{14}";
        return patternMatch(fileName, regex);
    }

    private static boolean validateFileName(String fileName, String timestamp) {
        logger.info("{}.{} - Validating extracted fileName: {} and timestamp: {}", FileCheck.class.getSimpleName(), "validateFileName", fileName, timestamp);
        return fileNameCheck(fileName) && timestampCheck(timestamp);
    }

    //Auto clears redis at 12:00 AM daily
    @Scheduled(cron = "0 0 0 * * ?")
    public boolean clear() {
        logger.info("{}.{} - Scheduled task started to clear Redis cache.", FileCheck.class.getSimpleName(), "clear");
        try {
            redisTemplate.getConnectionFactory().getConnection().flushAll();
            logger.info("{}.{} - Redis cache cleared successfully.", FileCheck.class.getSimpleName(), "clear");
            return true;
        } catch (Exception e) {
            logger.error("{}.{} - Failed to clear Redis cache: {}", FileCheck.class.getSimpleName(), "clear", e.getMessage(), e);
            return false;
        }
    }


    public static boolean timestampCheck(String timestamp) {
        logger.info("{}.{} - Validating timestamp: {}", FileCheck.class.getSimpleName(), "timestampCheck", timestamp);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

        try {
            LocalDateTime.parse(timestamp, formatter);
            logger.info("{}.{} - Timestamp validation passed for: {}", FileCheck.class.getSimpleName(), "timestampCheck", timestamp);
            return true;
        } catch (DateTimeParseException e) {
            logger.error("{}.{} - Timestamp validation failed for: {}. Error: {}", FileCheck.class.getSimpleName(), "timestampCheck", timestamp, e.getMessage(), e);
            return false;
        }
    }


    public static String patternMatch(String fileName, String regex) {
        logger.info("{}.{} - Matching fileName: '{}' with regex: '{}'", FileCheck.class.getSimpleName(), "patternMatch", fileName, regex);

        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(fileName);

        if (matcher.find()) {
            String matchedPart = matcher.group(1); // Group 1 contains the desired part
            logger.info("{}.{} - Match found: '{}'", FileCheck.class.getSimpleName(), "patternMatch", matchedPart);
            return matchedPart;
        } else {
            logger.warn("{}.{} - No match found for fileName: '{}' with regex: '{}'", FileCheck.class.getSimpleName(), "patternMatch", fileName, regex);
            return null;
        }
    }


    public static Boolean fileNameCheck(String fileName) {
        logger.info("{}.{} - Checking file name: {}", FileCheck.class.getSimpleName(), "fileNameCheck", fileName);
        try {
            JsonNode fileNameArray = getJson("fileName");
            return isFileNamePresent(fileNameArray, fileName);
        } catch (Exception e) {
            logger.error("{}.{} - Error checking file name: {}", FileCheck.class.getSimpleName(), "fileNameCheck", e.getMessage(), e);
            return false;
        }
    }

    private static Boolean isFileNamePresent(JsonNode fileNameArray, String fileName) {
        logger.debug("{}.{} - Validating file name presence in array: {}", FileCheck.class.getSimpleName(), "isFileNamePresent", fileName);
        return Optional.ofNullable(fileNameArray)
                .filter(JsonNode::isArray)
                .map(array -> StreamSupport.stream(array.spliterator(), false)
                        .map(JsonNode::asText)
                        .anyMatch(name -> name.contains(fileName)))
                .orElse(false);
    }


    public static JsonNode getJson(String property) throws IOException {
        logger.info("{}.{} - Retrieving JSON for property: {}", FileCheck.class.getSimpleName(), "getJson", property);

        try (InputStream inputStream = FileCheck.class.getResourceAsStream("/config.json")) {
            if (inputStream == null) {
                logger.error("{}.{} - Resource '/config.json' not found.", FileCheck.class.getSimpleName(), "getJson");
                throw new IOException("Resource '/config.json' not found.");
            }

            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(inputStream);
            JsonNode propertyNode = rootNode.path(property);

            logger.debug("{}.{} - Retrieved JSON for property '{}': {}", FileCheck.class.getSimpleName(), "getJson", property, propertyNode);
            return propertyNode;
        } catch (IOException e) {
            logger.error("{}.{} - Error reading JSON resource for property '{}': {}", FileCheck.class.getSimpleName(), "getJson", property, e.getMessage(), e);
            throw e;
        }
    }

}
