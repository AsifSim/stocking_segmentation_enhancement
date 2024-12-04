package com.sim.spriced.platform;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sim.spriced.platform.Pojo.WrapperRequest;
import com.sim.spriced.platform.Pojo.WrapperResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

@RestController
public class Wrapper {

    private static final Logger logger = LoggerFactory.getLogger(Wrapper.class);
    boolean flag=true;
    String message="";
    WrapperResponse response;
    @PostMapping("wrapper/egress")//changed
    public ResponseEntity responseListener(@RequestBody String resBody) throws JsonProcessingException {
        logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
        logger.info("Parameter(jsonReq = {})",resBody);
        flag=false;
        message=resBody;
        ObjectMapper objectMapper = new ObjectMapper();
         // Parse the JSON string into a Map
        response = objectMapper.readValue(resBody, WrapperResponse.class);
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return new ResponseEntity(response, HttpStatus.OK);
    }

    @PostMapping("/platform")//changed
    public ResponseEntity requestListener(@RequestBody WrapperRequest reqBody) throws InterruptedException {
        logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
        logger.info("Parameter(jsonReq = {})",reqBody);
        try {
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI("http://127.0.0.1:8090/org.example/greetings/842")) // Replace with your target URL
                    .PUT(HttpRequest.BodyPublishers.ofString(new ObjectMapper().writeValueAsString(reqBody), StandardCharsets.UTF_8))
                    .header("Content-Type", "application/vnd.greeter.types/Greet")
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            System.out.println("Response from target endpoint: " + response.body());
        } catch (Exception e) {
            e.printStackTrace();
        }
        while(flag){
            System.out.println("===========in while, flag="+flag);
            Thread.sleep(2000);
        }
        flag=true;
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return new ResponseEntity(response, HttpStatus.OK);
    }
}
