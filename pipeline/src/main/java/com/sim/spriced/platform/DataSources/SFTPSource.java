package com.sim.spriced.platform.DataSources;

import com.sim.spriced.platform.DataSources.SourceReader.SFTPSourceReader;
import com.sim.spriced.platform.DataSources.Split.SFTPSPlit;
import com.sim.spriced.platform.DataSources.Split.SFTPSplitSerializer;
import com.sim.spriced.platform.DataSources.SplitEnumerator.SFTPSplitEnumerator;
import com.sim.spriced.platform.Pojo.PricePojo;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.Properties;

public class SFTPSource implements Source<PricePojo, SFTPSPlit, Void> {

    private final Properties properties;

    public SFTPSource(Properties properties) {
        this.properties = properties;
    }


    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<SFTPSPlit, Void> createEnumerator(SplitEnumeratorContext<SFTPSPlit> splitEnumeratorContext) throws Exception {
        return new SFTPSplitEnumerator(splitEnumeratorContext);
    }

    @Override
    public SplitEnumerator<SFTPSPlit, Void> restoreEnumerator(SplitEnumeratorContext<SFTPSPlit> splitEnumeratorContext, Void unused) throws Exception {
        return new SFTPSplitEnumerator(splitEnumeratorContext);
    }

    @Override
    public SimpleVersionedSerializer<SFTPSPlit> getSplitSerializer() {
        return new SFTPSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
        return new VoidSerializer();
    }

    @Override
    public SourceReader<PricePojo, SFTPSPlit> createReader(SourceReaderContext sourceReaderContext) throws Exception {
        return new SFTPSourceReader(this.properties, sourceReaderContext);
    }
}
