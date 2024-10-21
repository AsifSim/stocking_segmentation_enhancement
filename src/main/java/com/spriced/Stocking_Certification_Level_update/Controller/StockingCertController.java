package com.spriced.Stocking_Certification_Level_update.Controller;

import com.spriced.Stocking_Certification_Level_update.DTO.Request.JsonRequest;
import com.spriced.Stocking_Certification_Level_update.DTO.Request.StockingRequest;
import com.spriced.Stocking_Certification_Level_update.DTO.Response.PartResponse;
import com.spriced.Stocking_Certification_Level_update.Service.StockingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StockingCertController {

    @Autowired
    StockingService stockServ;

    @GetMapping("/updateStockingCert")
    public ResponseEntity update(@RequestBody JsonRequest reqBody){
        System.out.println("=============Inside controller updateStockingCert");
        PartResponse partresp;
        try{
            partresp=stockServ.update(reqBody);
        }
        catch(Exception e){
            return new ResponseEntity(e.getMessage(),HttpStatus.OK);
        }
        return new ResponseEntity(partresp, HttpStatus.OK);
    }
}
