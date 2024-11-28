package com.spriced.Stocking_Certification_Level_update.Controller;

import com.spriced.Stocking_Certification_Level_update.DTO.Request.JsonRequest;
import com.spriced.Stocking_Certification_Level_update.DTO.Request.StockingRequest;
import com.spriced.Stocking_Certification_Level_update.DTO.Response.PartResponse;
import com.spriced.Stocking_Certification_Level_update.Exceptions.CurrentPartNumberNotFoundException;
import com.spriced.Stocking_Certification_Level_update.Exceptions.IdDoesntExistException;
import com.spriced.Stocking_Certification_Level_update.Exceptions.NoMatchingEccWithPartException;
import com.spriced.Stocking_Certification_Level_update.Exceptions.NoStockingCertForPartException;
import com.spriced.Stocking_Certification_Level_update.Exceptions.PartNotFoundException;
import com.spriced.Stocking_Certification_Level_update.Service.StockingCertificationService;
import com.spriced.Stocking_Certification_Level_update.Service.StockingSegmentCTTService;
import com.spriced.Stocking_Certification_Level_update.Service.StockingSegmentEBUService;
import com.spriced.Stocking_Certification_Level_update.Service.StockingSegmentHHPService;
import com.spriced.Stocking_Certification_Level_update.Service.Implementation.StockingCertificationServImpl;
import com.spriced.Stocking_Certification_Level_update.Service.Implementation.StockingSegmentCTTServiceImpl;
import com.spriced.Stocking_Certification_Level_update.Service.Implementation.StockingSegmentEBUServiceImpl;
import com.spriced.Stocking_Certification_Level_update.Service.Implementation.StockingSegmentHHPServiceImpl;
import com.spriced.workflow.DataReading;

import flink.generic.db.Model.Condition;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StockingCertController {

    @Autowired
    StockingCertificationServImpl stockCertServ;
    @Autowired
    StockingSegmentCTTServiceImpl stockSegCTTServ;
    @Autowired
    StockingSegmentEBUServiceImpl stockSegEBUServ;
    @Autowired
    StockingSegmentHHPServiceImpl stockSegHHPServ;
    private static final Logger logger = LoggerFactory.getLogger(StockingCertificationServImpl.class);
    
    @Autowired
    DataReading genServ;

    @GetMapping("/stock-enhance-file")
    public ResponseEntity update(@RequestBody JsonRequest reqBody){
    	logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    	logger.info("Parameter(reqBody = {})",reqBody);
        PartResponse partresp;
        try{
            partresp=stockCertServ.update(reqBody);
        }
        catch(Exception e){
        	logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
            return new ResponseEntity(e.getMessage(),HttpStatus.OK);
        }
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return new ResponseEntity(partresp, HttpStatus.OK);
    }
    
    @GetMapping("/stock-enhance-ui")
    public ResponseEntity update(@RequestParam String partNumber) throws PartNotFoundException, NoStockingCertForPartException, NoMatchingEccWithPartException, IdDoesntExistException, CurrentPartNumberNotFoundException{//change to reqBody
    	logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    	logger.info("Parameter(partNumber = {})",partNumber);
    	String stockingSegment="";
    	List<Condition> conditions = new ArrayList<>(List.of(new Condition("code",partNumber,"=","AND")));
    	List<String> columns = new ArrayList<>(List.of("product_bu"));
        logger.info("Going to execute read query from part entity");
        String productBu = (String)genServ.read("part",columns,conditions,"spriced_meritor").get(0).get("product_bu");
        logger.info("Read query from part entity has been executed");
        
        if(productBu.equalsIgnoreCase("EBU")) {
        	stockingSegment=stockSegEBUServ.update(partNumber);
        }
        else if(productBu.equalsIgnoreCase("HHP")) {
        	stockingSegment=stockSegHHPServ.update(partNumber);
        }
        else if(productBu.equalsIgnoreCase("CTT")) {
        	stockingSegment=stockSegCTTServ.update(partNumber,productBu);
        }
        return new ResponseEntity(stockingSegment, HttpStatus.OK);
    }
    
}
