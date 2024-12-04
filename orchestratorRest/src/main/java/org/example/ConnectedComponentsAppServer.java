package org.example;

import io.undertow.Undertow;
import org.apache.flink.statefun.sdk.java.StatefulFunctions;
import org.apache.flink.statefun.sdk.java.handler.RequestReplyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class ConnectedComponentsAppServer {
//    private static final Logger logger = LoggerFactory.getLogger(ConnectedComponentsAppServer.class);

    public static void main(String[] args) throws IOException {
        Properties properties= new Properties();
        InputStream input = ConnectedComponentsAppServer.class.getClassLoader().getResourceAsStream("application.properties");
        properties.load(input);
        System.out.println("=========Inside main");
//        logger.info("=============Inside Class ConnectedComponentsAppServer, Function main");
        final StatefulFunctions functions = new StatefulFunctions();
        functions.withStatefulFunction(PlatformStatefulFn.SPEC);
        System.out.println("=========Inside main 24");
//        logger.info("Configuring the Http listener for stateful function");
//        logger.info("function = {}",functions.toString());
        final RequestReplyHandler requestReplyHandler = functions.requestReplyHandler();
//        logger.info("requestReplyHandler = {}",requestReplyHandler.toString());
//        logger.info("port = {}, host = {}",Integer.parseInt(properties.getProperty("StatefulFnPORT")),properties.getProperty("StatefulFnHOST"));
        final Undertow httpServer =
                Undertow.builder().addHttpListener(Integer.parseInt(properties.getProperty("StatefulFnPORT")),properties.getProperty("StatefulFnHOST")).setHandler(new org.example.UndertowHttpHandler(requestReplyHandler)).build();
//        logger.info("Http listener configuration is done, httpServer = {}",httpServer.toString());
//        logger.info("Http listener for stateful function is starting");
        System.out.println("=========Inside main starting server");
        httpServer.start();
    }
}
