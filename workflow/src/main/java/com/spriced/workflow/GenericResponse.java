package com.spriced.workflow;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GenericResponse {
    private String operation;//read upsert insert update delete
    private String entityName;
    private List<Map<String,Object>> values;
    private String exception;
}
