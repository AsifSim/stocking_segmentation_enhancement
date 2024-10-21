package com.spriced.Stocking_Certification_Level_update.DTO.Response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PartResponse {
    String partNumber;
    String suggestedStockingCert;
    String entity;
    String operation;
}
