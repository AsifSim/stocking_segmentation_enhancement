package com.sim.spriced.platform.Pipeline;

import com.jcraft.jsch.ChannelSftp;
import com.sim.spriced.platform.Connectors.SFTPConnector;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.Properties;

public class PlatformPipeline {

    private static final Logger logger = LoggerFactory.getLogger(PlatformPipeline.class);

    public PlatformPipeline() {
        logger.info("{}.{} initialized", this.getClass().getSimpleName(), "PlatformPipeline");
    }

    public void initSourceConnector(Properties properties) {
        logger.info("{}.{} called", this.getClass().getSimpleName(), "initSourceConnector");

        try {
            logger.debug("{}.{} - Creating SFTPConnector instance", this.getClass().getSimpleName(), "initSourceConnector");
            SFTPConnector sftpConnector = new SFTPConnector();
            logger.debug("{}.{} - SFTPConnector instance created successfully", this.getClass().getSimpleName(), "initSourceConnector");

            logger.debug("{}.{} - Initializing connection with provided properties", this.getClass().getSimpleName(), "initSourceConnector");
            sftpConnector.initConnection(properties);
            logger.info("{}.{} - SFTP connection established successfully", this.getClass().getSimpleName(), "initSourceConnector");

            String sourcePath = properties.getProperty(SFTPConnector.SOURCE);
            String destinationPath = properties.getProperty(SFTPConnector.DESTINATION);

            logger.debug("{}.{} - Fetching files from source: {} to destination: {}", this.getClass().getSimpleName(), "initSourceConnector", sourcePath, destinationPath);
            sftpConnector.getFileForDate(sourcePath, OffsetDateTime.now(), destinationPath);
            logger.info("{}.{} - Files downloaded to {}", this.getClass().getSimpleName(), "initSourceConnector", destinationPath);

            logger.debug("{}.{} - Closing SFTP connection", this.getClass().getSimpleName(), "initSourceConnector");
            sftpConnector.closeConnection();
            logger.info("{}.{} - SFTP connection closed successfully", this.getClass().getSimpleName(), "initSourceConnector");

        } catch (Exception ex) {
            logger.error("{}.{} - Exception occurred while establishing connection and fetching file: {}", this.getClass().getSimpleName(), "initSourceConnector", ex.getMessage(), ex);
            ex.printStackTrace();
        }
    }

    public void initFlinkIngress() {
        logger.info("{}.{} called", this.getClass().getSimpleName(), "initFlinkIngress");

    }

    public void initFLinkEgress() {
        logger.info("{}.{} called", this.getClass().getSimpleName(), "initFLinkEgress");

    }
}

