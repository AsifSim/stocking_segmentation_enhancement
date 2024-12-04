package com.sim.spriced.platform.Connectors;

import com.jcraft.jsch.*;
import com.sim.spriced.platform.Exceptions.FormatException;
import com.sim.spriced.platform.Utility.RetryUtility;
import jakarta.annotation.PostConstruct;
import lombok.SneakyThrows;
import com.sim.spriced.platform.Exceptions.MissingPropertiesException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.Properties;
import java.util.Vector;

public class SFTPConnector implements CustomConnectors<ChannelSftp>{

    public static final String USERNAME = "username";
    public static final String HOST = "host";
    public static final String PORT = "port";
    private static final String STRICT_HOST_KEY_CHECKING = "StrictHostKeyChecking";
    public  static final String SOURCE = "source";
    public static final String DESTINATION = "destination";

    private static final String key = "C:\\Users\\aaditya.bhardwaj_sim\\.ssh\\id_rsa";
    private static final int DEFAULT_PORT = 22;
    private static final String DEFAULT_NO_VALUE = "no";
    private static final String DEFAULT_YES_VALUE = "yes";

    private final JSch jSch;
    private Session session;
    private ChannelSftp channelSftp;

    private static final Logger LOG = LoggerFactory.getLogger(SFTPConnector.class);

    @SneakyThrows
    public SFTPConnector() {
        logger.info("Executing {}.{}", this.getClass().getSimpleName(), "SFTPConnector");
        try {
            jSch = new JSch();
            jSch.addIdentity(key);
            logger.debug("{}.{} initialized successfully with key: {}", this.getClass().getSimpleName(), "SFTPConnector", key);
        } catch (JSchException e) {
            logger.error("Error in {}.{}: {}", this.getClass().getSimpleName(), "SFTPConnector", e.getMessage(), e);
            throw e; // Let SneakyThrows rethrow the exception
        }
    }

    @Override
    public void initConnection(Properties properties) {
        logger.info("Executing {}.{}", this.getClass().getSimpleName(), "initConnection");
        try {
            validateProperties(properties);
            createSession(properties);
            createSFTPConnection();
            logger.debug("{}.{} - Connection initialized successfully with properties: {}", this.getClass().getSimpleName(), "initConnection", properties);
        } catch (MissingPropertiesException e) {
            logger.error("Error in {}.{}: {}", this.getClass().getSimpleName(), "initConnection", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }


    public void getFile(String src, String dst) throws SftpException {
        logger.info("Executing {}.{} with src: {}, dst: {}", this.getClass().getSimpleName(), "getFile", src, dst);
        if (this.channelSftp.isConnected()) {
            try {
                this.channelSftp.get(src, dst);
                logger.debug("{}.{} - File transferred successfully from src: {} to dst: {}", this.getClass().getSimpleName(), "getFile", src, dst);
            } catch (SftpException e) {
                logger.error("Error in {}.{}: {}", this.getClass().getSimpleName(), "getFile", e.getMessage(), e);
                throw e;
            }
        } else {
            logger.error("{}.{} - SFTP is not connected.", this.getClass().getSimpleName(), "getFile");
            throw new RuntimeException("SFTP is not connected");
        }
    }


    public void getFileForDate(String remoteDir, OffsetDateTime date, String dst) {
        try {
            validateRemoteDir(remoteDir);
            ensureSFTPConnected();
            String formattedDate = formatDate(date);
            processFiles(remoteDir, formattedDate, dst);
        } catch (SftpException | FormatException e) {
            logger.error("{}.{} - Exception occurred: {}", this.getClass().getSimpleName(), "getFileForDate", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private void validateRemoteDir(String remoteDir) throws FormatException {
        logger.info("{}.{} - Validating remote directory: {}", this.getClass().getSimpleName(), "validateRemoteDir", remoteDir);
        if (remoteDir.contains(".")) {
            logger.error("{}.{} - Invalid directory path: {}", this.getClass().getSimpleName(), "validateRemoteDir", remoteDir);
            throw new FormatException("The directory path cannot contain file name");
        }
    }

    private void ensureSFTPConnected() {
        logger.info("{}.{} - Checking SFTP connection.", this.getClass().getSimpleName(), "ensureSFTPConnected");
        if (!this.channelSftp.isConnected()) {
            logger.error("{}.{} - SFTP is not connected.", this.getClass().getSimpleName(), "ensureSFTPConnected");
            throw new RuntimeException("SFTP is not connected");
        }
    }

    private String formatDate(OffsetDateTime date) {
        logger.info("{}.{} - Formatting date: {}", this.getClass().getSimpleName(), "formatDate", date);
        return date.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
    }

    private void processFiles(String remoteDir, String formattedDate, String dst) throws SftpException {
        logger.info("{}.{} - Processing files in remoteDir: {} for date: {}", this.getClass().getSimpleName(), "processFiles", remoteDir, formattedDate);
        this.channelSftp.ls(remoteDir).stream()
                .map(ChannelSftp.LsEntry::getFilename)
                .filter(fileName -> fileName.contains(formattedDate))
                .forEach(fileName -> transferFile(remoteDir, fileName, dst));
    }

    private void transferFile(String remoteDir, String fileName, String dst) {
        try {
            logger.info("{}.{} - Transferring file: {}", this.getClass().getSimpleName(), "transferFile", fileName);
            this.getFile(remoteDir + "/" + fileName, dst);
        } catch (SftpException e) {
            logger.error("{}.{} - Error transferring file {}: {}", this.getClass().getSimpleName(), "transferFile", fileName, e.getMessage(), e);
        }
    }


    public void putFile(String src, String dst) throws SftpException {
        logger.info("Executing {}.{} with src: {}, dst: {}", this.getClass().getSimpleName(), "putFile", src, dst);
        if (this.channelSftp.isConnected()) {
            try {
                logger.debug("{}.{} - SFTP connection is active. Transferring file.", this.getClass().getSimpleName(), "putFile");
                this.channelSftp.put(src, dst);
                logger.info("{}.{} - File successfully transferred from {} to {}", this.getClass().getSimpleName(), "putFile", src, dst);
            } catch (SftpException e) {
                logger.error("{}.{} - Error transferring file: {}", this.getClass().getSimpleName(), "putFile", e.getMessage(), e);
                throw e;
            }
        } else {
            logger.error("{}.{} - SFTP is not connected.", this.getClass().getSimpleName(), "putFile");
            throw new RuntimeException("SFTP is not connected");
        }
    }


    private void createSFTPConnection() {
        if (session.isConnected()) {
            try {
                initializeChannelSftp();
                connectWithRetry();
            } catch (Exception e) {
                logger.error("{}.{} - Error creating SFTP connection: {}", this.getClass().getSimpleName(), "createSFTPConnection", e.getMessage(), e);
                throw new RuntimeException("Failed to create SFTP connection.", e);
            }
        } else {
            logger.error("{}.{} - Session is disconnected. Please try again!", this.getClass().getSimpleName(), "createSFTPConnection");
            throw new RuntimeException("Session is disconnected. Please try again!");
        }
    }

    private void initializeChannelSftp() throws Exception {
        logger.debug("{}.{} - Initializing SFTP channel.", this.getClass().getSimpleName(), "initializeChannelSftp");
        this.channelSftp = (ChannelSftp) session.openChannel("sftp");
    }

    private void connectWithRetry() throws Exception {
        logger.debug("{}.{} - Connecting SFTP channel with retry.", this.getClass().getSimpleName(), "connectWithRetry");
        RetryUtility.retryConnections(() -> {
            this.channelSftp.connect();
            return null;
        }, 5, 10, "SFTP Connection");
    }

    private void createSession(Properties properties) {
        try {
            initializeSession(properties);
            connectSessionWithRetry();
        } catch (Exception e) {
            logger.error("{}.{} - Error while creating session: {}", this.getClass().getSimpleName(), "createSession", e.getMessage(), e);
            throw new RuntimeException("Exception while creating session - " + e.getMessage(), e);
        }
    }

    private void initializeSession(Properties properties) throws Exception {
        logger.debug("{}.{} - Initializing JSCH session with properties.", this.getClass().getSimpleName(), "initializeSession");
        session = jSch.getSession(
                properties.get(USERNAME).toString(),
                properties.get(HOST).toString(),
                getPort(properties.get(PORT))
        );
        session.setConfig(getPropertiesForSession(properties));
        logger.info("JSCH session initialized successfully.");
    }

    private void connectSessionWithRetry() throws Exception {
        logger.debug("{}.{} - Connecting JSCH session with retry.", this.getClass().getSimpleName(), "connectSessionWithRetry");
        RetryUtility.retryConnections(() -> {
            session.connect();
            return null;
        }, 5, 10, "JSCH Session");
        logger.info("JSCH session connected successfully.");
    }


    private Properties getPropertiesForSession(Properties properties) {
        logger.debug("{}.{} - Preparing session properties.", this.getClass().getSimpleName(), "getPropertiesForSession");
        Properties config = new Properties();
        logger.debug("{}.{} - Adding STRICT_HOST_KEY_CHECKING property.", this.getClass().getSimpleName(), "getPropertiesForSession");
        config.put(STRICT_HOST_KEY_CHECKING, properties.getOrDefault(STRICT_HOST_KEY_CHECKING, DEFAULT_NO_VALUE));
        logger.info("{}.{} - Session properties prepared successfully.", this.getClass().getSimpleName(), "getPropertiesForSession");
        return config;
    }


    private int getPort(Object port) {
        logger.debug("{}.{} - Resolving port from object: {}", this.getClass().getSimpleName(), "getPort", port);
        if (Objects.isNull(port)) {
            logger.info("{}.{} - Port is null. Using default port: {}", this.getClass().getSimpleName(), "getPort", DEFAULT_PORT);
            return DEFAULT_PORT;
        }
        int resolvedPort = Integer.parseInt(Objects.toString(port));
        logger.info("{}.{} - Resolved port: {}", this.getClass().getSimpleName(), "getPort", resolvedPort);
        return resolvedPort;
    }

    private void validateProperties(Properties properties) throws MissingPropertiesException {
        logger.debug("{}.{} - Validating properties for SFTP connection.", this.getClass().getSimpleName(), "validateProperties");
        if (!areRequiredPropertiesPresent(properties)) {
            logger.error("{}.{} - Some properties are missing. Please check the configuration properly for SFTP connection.", this.getClass().getSimpleName(), "validateProperties");
            throw new MissingPropertiesException("Some properties are missing. Please check the configuration properly for SFTP connection.");
        }
        logger.info("{}.{} - Properties validated successfully.", this.getClass().getSimpleName(), "validateProperties");
    }

    private boolean areRequiredPropertiesPresent(Properties properties) {
        return properties.containsKey(USERNAME) && properties.containsKey(HOST)
                && properties.containsKey(SOURCE) && properties.containsKey(DESTINATION);
    }

    @Override
    public void closeConnection() {
        logger.info("Executing {}.{}", this.getClass().getSimpleName(), "closeConnection");
        closeSFTPChannel();
        closeSession();
        logger.info("{}.{} - Connection closed successfully.", this.getClass().getSimpleName(), "closeConnection");
    }

    private void closeSFTPChannel() {
        if (channelSftp != null && channelSftp.isConnected()) {
            logger.debug("{}.{} - Disconnecting SFTP channel.", this.getClass().getSimpleName(), "closeSFTPChannel");
            channelSftp.disconnect();
        } else {
            logger.debug("{}.{} - SFTP channel is already disconnected.", this.getClass().getSimpleName(), "closeSFTPChannel");
        }
    }

    private void closeSession() {
        if (session != null && session.isConnected()) {
            logger.debug("{}.{} - Disconnecting session.", this.getClass().getSimpleName(), "closeSession");
            session.disconnect();
        } else {
            logger.debug("{}.{} - Session is already disconnected.", this.getClass().getSimpleName(), "closeSession");
        }
    }


}
