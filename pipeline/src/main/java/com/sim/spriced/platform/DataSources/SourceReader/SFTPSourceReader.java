package com.sim.spriced.platform.DataSources.SourceReader;

import com.sim.spriced.platform.Connectors.SFTPConnector;
import com.sim.spriced.platform.Constants.Constants;
import com.sim.spriced.platform.DataSources.Split.SFTPSPlit;
import com.sim.spriced.platform.Pojo.Data;
import com.sim.spriced.platform.Pojo.PricePojo;
import com.sim.spriced.platform.Utility.BatchingUtility;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class SFTPSourceReader implements SourceReader<PricePojo, SFTPSPlit> {

    private final Properties properties;
    private final SourceReaderContext context;
    private boolean isFinished = false;

    public SFTPSourceReader(Properties properties, SourceReaderContext context) {
        this.properties = properties;
        this.context = context;
    }


    @Override
    public void start() {

    }

    @Override
    public InputStatus pollNext(ReaderOutput<PricePojo> readerOutput) throws Exception {
        if(isFinished)
        {
            return InputStatus.NOTHING_AVAILABLE;
        }
        else {
       InputStream inputStream = new FileInputStream(this.properties.getProperty(SFTPConnector.DESTINATION)+this.properties.getProperty(Constants.FILE_NAME));
            InputStreamReader reader = new InputStreamReader(inputStream);
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());
            Map<String,Integer> headers = csvParser.getHeaderMap();
            for(CSVRecord csvRecord: csvParser)
            {
                int i = 0;
                Data[] data = new Data[headers.keySet().size()];
                for(String header: headers.keySet())
                {
                    Data temp = new Data();
                    temp.key = header;
                    temp.value = csvRecord.get(header);
                    data[i] = temp;
                  //  System.out.println("************** " + temp.toString());
                   // System.out.println("The data for this row is" + data[i].toString());
                    i++;
                }
//                Arrays.stream(data).forEach(item -> {
//                    System.out.println("key - " + item.key);
//                    System.out.println("value - " + item.value);
//                });
                PricePojo pricePojo = new PricePojo(BatchingUtility.getBatchId(Constants.sourceMapping.get(Constants.FILE)),data);
              //  System.out.printf("*************************** %s %s **********************\n",pricePojo.batchID,pricePojo.data);
                readerOutput.collect(pricePojo);
            }
            csvParser.close();
            reader.close();
            isFinished = true;
            return InputStatus.END_OF_INPUT;
        }
    }

    @Override
    public List<SFTPSPlit> snapshotState(long l) {
        return List.of();
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return null;
    }

    @Override
    public void addSplits(List<SFTPSPlit> list) {

    }

    @Override
    public void notifyNoMoreSplits() {

    }

    @Override
    public void close() throws Exception {

    }

    private SFTPConnector initializeSFTPConnection()
    {
        SFTPConnector sftpConnector = new SFTPConnector();
        sftpConnector.initConnection(this.properties);
        return sftpConnector;
    }
}
