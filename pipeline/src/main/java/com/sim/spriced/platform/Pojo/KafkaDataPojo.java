package com.sim.spriced.platform.Pojo;

import java.util.Map;

public class KafkaDataPojo {

    private String batchID;
    private Map<String,Object> data;

    public String getBatchID() {
        return batchID;
    }

    public void setBatchID(String batchID) {
        this.batchID = batchID;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

}
