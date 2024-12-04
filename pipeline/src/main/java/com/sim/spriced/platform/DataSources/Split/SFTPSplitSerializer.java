package com.sim.spriced.platform.DataSources.Split;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SFTPSplitSerializer implements SimpleVersionedSerializer<SFTPSPlit> {

    private static final Logger logger = LoggerFactory.getLogger(SFTPSplitSerializer.class);
    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        logger.info("Executing {}.getVersion - Returning version: {}", this.getClass().getSimpleName(), VERSION);
        return VERSION;
    }

    @Override
    public byte[] serialize(SFTPSPlit sftpsPlit) throws IOException {
        logger.info("Executing {}.serialize - Serializing SFTPSPlit with ID: {}", this.getClass().getSimpleName(), sftpsPlit.splitId());
        byte[] serializedBytes = sftpsPlit.splitId().getBytes(StandardCharsets.UTF_8);
        logger.debug("{}.serialize - Serialized bytes length: {}", this.getClass().getSimpleName(), serializedBytes.length);
        return serializedBytes;
    }

    @Override
    public SFTPSPlit deserialize(int version, byte[] bytes) throws IOException {
        logger.info("Executing {}.deserialize - Deserializing SFTPSPlit for version: {} with byte array length: {}",
                this.getClass().getSimpleName(), version, bytes.length);
        if (version != VERSION) {
            String error = String.format("Unsupported version: %d. Expected version: %d", version, VERSION);
            logger.error("{}.deserialize - {}", this.getClass().getSimpleName(), error);
            throw new IOException(error);
        }
        String splitId = new String(bytes, StandardCharsets.UTF_8);
        logger.debug("{}.deserialize - Deserialized split ID: {}", this.getClass().getSimpleName(), splitId);
        return new SFTPSPlit(splitId);
    }

}
