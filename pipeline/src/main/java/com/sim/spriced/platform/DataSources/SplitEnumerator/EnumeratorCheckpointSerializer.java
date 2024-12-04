package com.sim.spriced.platform.DataSources.SplitEnumerator;

import com.sim.spriced.platform.DataSources.Split.SFTPSPlit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

//public class EnumeratorCheckpointSerializer implements SimpleVersionedSerializer<Void>{
//
//
//        private static final int VERSION = 1;
//        @Override
//        public int getVersion() {
//            return VERSION;
//        }
//
//        @Override
//        public byte[] serialize(SFTPSPlit sftpsPlit) throws IOException {
//            return sftpsPlit.splitId().getBytes(StandardCharsets.UTF_8);
//        }
//
//        @Override
//        public SFTPSPlit deserialize(int i, byte[] bytes) throws IOException {
//            String splitId = new String(bytes, StandardCharsets.UTF_8);
//            return new SFTPSPlit(splitId);
//        }
//    }


