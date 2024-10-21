package com.spriced.Stocking_Certification_Level_update.Transformer.DtoToModel;

import com.spriced.Stocking_Certification_Level_update.DTO.Response.PartResponse;

public class PartToPartResponse {
    public static PartResponse PartToPartResponse(String result,String part_Number,String entity,String operation){
        return PartResponse.builder()
                .suggestedStockingCert(result)
                .partNumber(part_Number)
                .operation(operation)
                .entity(entity)
                .build();
    }
}
