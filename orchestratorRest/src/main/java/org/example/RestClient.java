package org.example;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RestClient {
//    private static final Logger logger = LoggerFactory.getLogger(RestClient.class);
    public static String callRestEndpoint(String url) {
        System.out.println("=========Inside callRestEndpoint");
//        logger.info("=============Inside Class RestClient, Function callRestEndpoint");
//        logger.info("url = {}",url);
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet request = new HttpGet(url);
            try (CloseableHttpResponse response = httpClient.execute(request)) {
                HttpEntity entity = response.getEntity();
//                logger.info("entity = {}",entity);
//                logger.info("=============Going out of callRestEndpoint");
                if (entity != null) return EntityUtils.toString(entity);
                else return "No content in response";
            }
        } catch (IOException e) {
            e.printStackTrace();
//            logger.error("An error has occurred, Message = {}",e.getMessage());
//            logger.info("=============Going out of callRestEndpoint");
            return "Error: " + e.getMessage();
        }
    }
}

