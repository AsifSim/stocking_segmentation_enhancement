package com.sim.spriced.platform.Pojo;

import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

import java.util.HashMap;
import java.util.Map;

@Setter
@Getter
public class PricePojo {

    public String batchID;

    public Data[] data;

    public PricePojo(){};

    public PricePojo(String batchID, Data[] data)
    {
        this.batchID = batchID;
        this.data = data;
    }



}
