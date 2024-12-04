package com.sim.spriced.platform.DataSink;

import com.sim.spriced.platform.Pojo.PricePojo;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.api.connector.sink.Sink.InitContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.kafka.common.protocol.types.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Component
public class CSVSinkWriter {

    private static final Logger logger = LoggerFactory.getLogger(CSVSinkWriter.class);

    public static FileSink<String> getFileSink(String path, OutputFileConfig fileConfig) {
        logger.info("Executing {}.{} with path: {} and fileConfig: {}",
                CSVSinkWriter.class.getSimpleName(), "getFileSink", path, fileConfig);

        FileSink<String> fileSink = FileSink.<String>forRowFormat(new Path(path), new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withRolloverInterval(Duration.ofMinutes(1))
                        .withInactivityInterval(Duration.ofMinutes(1))
                        .build())
                .withOutputFileConfig(fileConfig)
                .withBucketAssigner(new BasePathBucketAssigner<>())
                .build();

        logger.info("{}.{} - FileSink created successfully with path: {}",
                CSVSinkWriter.class.getSimpleName(), "getFileSink", path);

        return fileSink;
    }

}