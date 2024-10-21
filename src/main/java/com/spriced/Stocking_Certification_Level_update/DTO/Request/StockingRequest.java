package com.spriced.Stocking_Certification_Level_update.DTO.Request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class StockingRequest {
    String part_number;
//    List<String> configuration;
    String configuration;
}
