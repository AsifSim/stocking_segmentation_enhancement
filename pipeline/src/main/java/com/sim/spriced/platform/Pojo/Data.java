package com.sim.spriced.platform.Pojo;

import lombok.Getter;

@Getter
public class Data {
    public String key;
    public String value;

    public void setValue(String value) {
        this.value = value;
    }

    public void setKey(String key) {
        this.key = key;
    }

    @Override
    public String toString() {
        return "Data{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
