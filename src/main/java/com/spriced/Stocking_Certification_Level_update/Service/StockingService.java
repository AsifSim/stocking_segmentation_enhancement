package com.spriced.Stocking_Certification_Level_update.Service;

import com.spriced.Stocking_Certification_Level_update.DTO.Request.JsonRequest;
import com.spriced.Stocking_Certification_Level_update.DTO.Request.StockingRequest;
import com.spriced.Stocking_Certification_Level_update.DTO.Response.PartResponse;
import org.springframework.stereotype.Service;

@Service
public interface StockingService {
    public PartResponse update(JsonRequest reqBody);
}
