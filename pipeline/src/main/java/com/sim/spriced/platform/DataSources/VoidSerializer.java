package com.sim.spriced.platform.DataSources;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

public class VoidSerializer implements SimpleVersionedSerializer<Void> {

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(Void obj) throws IOException {
        return new byte[0];
    }

    @Override
    public Void deserialize(int version, byte[] serialized) throws IOException {
        return null;
    }
}