package com.spriced.Stocking_Certification_Level_update.Controller;

import com.spriced.Stocking_Certification_Level_update.Exceptions.*;
import com.spriced.Stocking_Certification_Level_update.Service.Implementation.StockingCertificationServImpl;
import com.spriced.Stocking_Certification_Level_update.Service.Implementation.StockingSegmentCTTServiceImpl;
import com.spriced.Stocking_Certification_Level_update.Service.Implementation.StockingSegmentEBUServiceImpl;
import com.spriced.Stocking_Certification_Level_update.Service.Implementation.StockingSegmentHHPServiceImpl;
import com.spriced.workflow.DataReading;
import com.spriced.workflow.GenericResponse;
import flink.generic.db.Model.Condition;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StockingCertController {

    @Value("${CODE}")
    private String CODE;

    @Value("${PRODUCT_BU}")
    private String PRODUCT_BU;

    @Value("${db_name}")
    private String DB_NAME;

    @Value("${PART}")
    private String PART;

    @Value("${SUGGESTED_STOCKING_CERT}")
    private String SUGGESTED_STOCKING_CERT;

    @Value("${SUGGESTED_STOCKING_SEGMENT}")
    private String SUGGESTED_STOCKING_SEGMENT;

    @Value("${SUPP_PART_STOCKING_SEGMENT}")
    private String SUPP_PART_STOCKING_SEGMENT;

    @Value("${EQUALS}")
    private String EQUALS;

    @Value("${AND}")
    private String AND;

    @Value("${EBU}")
    private String EBU;

    @Value("${HHP}")
    private String HHP;

    @Value("${CTT}")
    private String CTT;

    @Autowired
    StockingCertificationServImpl stockCertServ;
    @Autowired
    StockingSegmentCTTServiceImpl stockSegCTTServ;
    @Autowired
    StockingSegmentEBUServiceImpl stockSegEBUServ;
    @Autowired
    StockingSegmentHHPServiceImpl stockSegHHPServ;
    private static final Logger logger = LoggerFactory.getLogger(StockingCertController.class);
    
    @Autowired
    DataReading genServ;
    
    @GetMapping("/stock-enhance-ui")
    public ResponseEntity update(@RequestParam String partNumber) {
    	logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    	logger.info("Parameter(partNumber = {})",partNumber);
    	List<Condition> conditions = new ArrayList<>(List.of(new Condition(CODE,partNumber,EQUALS,AND)));
    	List<String> columns = new ArrayList<>(List.of(PRODUCT_BU));
        logger.info("Going to execute read query from part entity");
        String productBu = (String)genServ.read(PART,columns,conditions,DB_NAME).get(0).get(PRODUCT_BU);
        logger.info("Read query from part entity has been executed");
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return calculateStockingSegment(productBu,partNumber);
    }

    public ResponseEntity calculateStockingSegment(String productBu,String partNumber) {
        logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
        logger.info("Parameter(partNumber = {}, productBu = {})",partNumber,productBu);
        String stockingSegment="",stockingCert="";
        try{
            stockingCert=stockCertServ.update(partNumber);
            if(productBu.equalsIgnoreCase(EBU)) stockingSegment=stockSegEBUServ.update(partNumber);
            else if(productBu.equalsIgnoreCase(HHP)) stockingSegment=stockSegHHPServ.update(partNumber);
//            else if(productBu.equalsIgnoreCase(CTT)) stockingSegment=stockSegCTTServ.update(partNumber,productBu);
            else if(productBu.equalsIgnoreCase(CTT)) stockingSegment=stockSegCTTServ.update(partNumber,productBu);

        }
        catch (PartNotFoundException | NoStockingCertForPartException | NoMatchingEccWithPartException | IdDoesntExistException | CurrentPartNumberNotFoundException |InvaliCertificationException e){
            logger.error("An Exception has occurred, Exception details = {}",e.getMessage());
            logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
            return new ResponseEntity(GenericResponse.builder().values(List.of(Map.of(SUGGESTED_STOCKING_SEGMENT, stockingSegment), Map.of(SUGGESTED_STOCKING_CERT, stockingCert))).entityName(SUPP_PART_STOCKING_SEGMENT).operation(null).exception(e.getMessage()).build(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        catch (RuntimeException e){
            logger.error("An Exception has occurred, Exception details = {}",e.getMessage());
            logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
            return new ResponseEntity(GenericResponse.builder().values(List.of(Map.of(SUGGESTED_STOCKING_SEGMENT, stockingSegment), Map.of(SUGGESTED_STOCKING_CERT, stockingCert))).entityName(SUPP_PART_STOCKING_SEGMENT).operation(null).exception(e.getMessage()).build(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        catch (Exception e){
            logger.error("An Exception has occurred, Exception details = {}",e.getMessage());
            logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
            return new ResponseEntity(GenericResponse.builder().values(List.of(Map.of(SUGGESTED_STOCKING_SEGMENT, stockingSegment), Map.of(SUGGESTED_STOCKING_CERT, stockingCert))).entityName(SUPP_PART_STOCKING_SEGMENT).operation(null).exception(e.getMessage()).build(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return new ResponseEntity(GenericResponse.builder().values(List.of(Map.of(SUGGESTED_STOCKING_SEGMENT, stockingSegment), Map.of(SUGGESTED_STOCKING_CERT, stockingCert))).entityName(SUPP_PART_STOCKING_SEGMENT).operation(null).exception(null).build(), HttpStatus.OK);
    }
    
}
