package com.spriced.Stocking_Certification_Level_update.DTO.Request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class JsonRequest {
    String batch_id;
    Map<String,StockingRequest> data;
}
