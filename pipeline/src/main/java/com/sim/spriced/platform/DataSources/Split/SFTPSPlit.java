package com.sim.spriced.platform.DataSources.Split;

import org.apache.flink.api.connector.source.SourceSplit;

public class SFTPSPlit implements SourceSplit {
    private final String splitID;

    public SFTPSPlit(String splitID) {
        this.splitID = splitID;
    }

    @Override
    public String splitId() {
        return this.splitID;
    }
}
