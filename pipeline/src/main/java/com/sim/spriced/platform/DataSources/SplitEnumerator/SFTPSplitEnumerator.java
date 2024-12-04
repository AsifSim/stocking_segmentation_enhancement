package com.sim.spriced.platform.DataSources.SplitEnumerator;

import com.sim.spriced.platform.DataSources.SFTPSource;
import com.sim.spriced.platform.DataSources.Split.SFTPSPlit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

public class SFTPSplitEnumerator implements SplitEnumerator<SFTPSPlit,Void> {

    private final SplitEnumeratorContext<SFTPSPlit> context;
    private boolean splitAssigned = false;

    public SFTPSplitEnumerator(SplitEnumeratorContext<SFTPSPlit> context) {
        this.context = context;
    }

    @Override
    public void start() {

    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String s) {
        if (!splitAssigned) {
            SFTPSPlit split = new SFTPSPlit("sftp-split");
            context.assignSplit(split, subtaskId);
            context.signalNoMoreSplits(subtaskId);
            splitAssigned = true;
        }
    }

    @Override
    public void addSplitsBack(List<SFTPSPlit> list, int i) {
         splitAssigned = true;
    }

    @Override
    public void addReader(int i) {
         handleSplitRequest(i,null);
    }

    @Override
    public Void snapshotState(long l) throws Exception {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
