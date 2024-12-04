package com.sim.spriced.platform.Operations;

import com.sim.spriced.platform.Pojo.Data;
import com.sim.spriced.platform.Pojo.PricePojo;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JSONCreator extends RichMapFunction<PricePojo, PricePojo> {

    private static final Logger logger = LoggerFactory.getLogger(JSONCreator.class);

    @Override
    public PricePojo map(PricePojo pricePojo) throws Exception {
        logger.info("{}.{} - Mapping PricePojo with BatchID: {}", this.getClass().getSimpleName(), "map", pricePojo.getBatchID());
        performDummyOperations(pricePojo);
        return pricePojo;
    }

    public void performDummyOperations(PricePojo pricePojo) {
        logger.debug("{}.{} - Performing dummy operations on BatchID: {}", this.getClass().getSimpleName(), "performDummyOperations", pricePojo.getBatchID());
    }
}