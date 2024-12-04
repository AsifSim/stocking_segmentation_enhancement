package com.sim.spriced.platform.Pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class WrapperRequest {
    float version;
    List<Map<String,Object>> payload;
}
