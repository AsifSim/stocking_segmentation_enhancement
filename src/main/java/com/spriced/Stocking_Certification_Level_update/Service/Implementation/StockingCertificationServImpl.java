package com.spriced.Stocking_Certification_Level_update.Service.Implementation;

import com.spriced.Stocking_Certification_Level_update.DTO.Request.StockingRequest;
import com.spriced.Stocking_Certification_Level_update.DTO.Request.JsonRequest;
import com.spriced.Stocking_Certification_Level_update.DTO.Response.PartResponse;
import com.spriced.Stocking_Certification_Level_update.Service.StockingCertificationService;
import com.spriced.Stocking_Certification_Level_update.Transformer.DtoToModel.PartToPartResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import flink.generic.db.Model.Condition;
import flink.generic.db.Service.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.spriced.workflow.*;

@Service
public class StockingCertificationServImpl implements StockingCertificationService {
    
    @Autowired
    DataReading genServ;
    private static final Logger logger = LoggerFactory.getLogger(StockingCertificationServImpl.class);

    public PartResponse update(JsonRequest jsonReq){
    	logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    	logger.info("Parameter(jsonReq = {})",jsonReq);
        List<String> configurations=new ArrayList<>();
        Optional<String> partOpt = jsonReq.getData().values().stream().map(StockingRequest::getPart_number).findFirst();
        String partNumber=partOpt.get();
        logger.info("part_number = {}",partNumber);
        configurations.addAll(jsonReq.getData().values().stream().filter(reqBody -> !reqBody.getConfiguration().isEmpty()).map(reqBody -> reqBody.getConfiguration()).collect(Collectors.toList()));
        logger.info("configuration list = {}",configurations.toString());
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return getPartResponse(partNumber,configurations);
    }

    public Long findMin(Long min, Long rank){
    	logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    	logger.info("Parameter(min = {}, rank = {})",min,rank);
        logger.info("=============Going out of findMin");
        if(min==null) {
        	logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        	return rank;
        }
        else {
        	logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        	return ((min<rank)?rank:min);
        }
    }
    
    public Long getRank(String columnName,String stockingCertLevel) {
    	logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    	logger.info("Parameter(columnName = {}, stockingCertLevel = {})",columnName,stockingCertLevel);
    	List<Condition> conditions = new ArrayList<>(List.of(new Condition("certification",stockingCertLevel,"=","AND")));
    	List<String> columns = new ArrayList<>(List.of(columnName));
        logger.info("Going to execute read query from stocking_master_data entity");
        Long rank = (Long)genServ.read("p_stock_cert",columns,conditions,"spriced_meritor").get(0).get(columnName);
        logger.info("Read query from stocking_master_data entity has been executed");
        logger.info("rank {}",rank);
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return rank;
    }
    
    public String resultWhenNoConfiguration(Long certificationLevel) {
    	logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    	logger.info("Parameter(certificationLevel = {})",certificationLevel);
        List<Condition> conditions = new ArrayList<>(List.of(new Condition("id",certificationLevel,"=","AND")));        
        List<String> columns = new ArrayList<>(List.of("name"));
        logger.info("Going to execute read query from xcertification_level entity");
        String result=(String)genServ.read("xcertification_level",columns,conditions,"spriced_meritor").get(0).get("name");
        logger.info("Read query from xcertification_level entity has been executed");
        logger.info("result = {}",result);
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return result;
    }
    
    public String getResult(List<String> configurations,String productBu) {//------------can be broken further
    	logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    	logger.info("Parameter(configurations = {}, productBu = {})",configurations.toString(),productBu);    	
    	AtomicReference<Long> min = new AtomicReference<>(null);
        StringBuilder configWithoutCert = new StringBuilder();
        configurations.stream()
            .peek(config -> logger.info("Inside the service in loop"))
            .forEach(config -> {
                List<Condition> conditions = new ArrayList<>(List.of(new Condition("code", config, "=", "AND")));
                List<String> columns = new ArrayList<>(List.of("stocking_certification_level"));
                logger.info("Going to execute read query from configuration entity");
                String stockingCertLevel = (String) genServ.read("configuration", columns, conditions, "spriced_meritor").get(0).get("stocking_certification_level");
                logger.info("Read query from configuration entity has been executed");
                logger.info("stockingCertLevel = {}", stockingCertLevel);
                if (stockingCertLevel == null) {
                    logger.info("StockingCertLevel is null");
                    synchronized (configWithoutCert) {
                        configWithoutCert.append(config).append(",");
                    }
                } else {
                    long currentMin = getMin(configWithoutCert.toString(), productBu, stockingCertLevel, min.get());
                    min.set(currentMin);
                }
            });
        logger.info("min = {}",min.get());
        logger.info("configWithoutCert = {}",configWithoutCert);
        if (configWithoutCert.length() == 0) {
            logger.info("=============Going out of {}", (new Object() {}.getClass().getEnclosingMethod().getName()));
            return String.valueOf(min.get());
        } else {
            logger.info("=============Going out of {}", (new Object() {}.getClass().getEnclosingMethod().getName()));
            return "CHECK, " + configWithoutCert.substring(0, configWithoutCert.length() - 1);
        }
    }
    
    public PartResponse getPartResponse(String partNumber,List<String> configurations) {
    	logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    	logger.info("partNumber = {}, configurations = {}",partNumber,configurations.toString());
    	List<Condition> conditions = new ArrayList<>(List.of(new Condition("code", partNumber, "=", "AND")));
        List<String> columns = new ArrayList<>(List.of("id", "certification_level", "code", "suggested_stocking_cert", "product_bu"));
        logger.info("Going to execute read query from part entity");
        List<Map<String,Object>> part=genServ.read("part",columns,conditions,"spriced_meritor");
        logger.info("Read query from part entity has been executed");
        Long certificationLevel=(Long)(part.get(0)).get("certification_level");
        logger.info("certificationLevel = {}",certificationLevel);
        String productBu=(String)(part.get(0)).get("product_bu");
        logger.info("productBu = {}",productBu);
        String result=configurations.isEmpty()?resultWhenNoConfiguration(certificationLevel):getResult(configurations,productBu);
        logger.info("result = {}",result);
        logger.info("=============Going out of update");
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return PartToPartResponse.PartToPartResponse(result,partNumber,"supp_part_stocking_segment","update");
    }
    
    public Long getMin(String configWithoutCert, String productBu, String stockingCertLevel, Long min) {
    	logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    	logger.info("configWithoutCert = {}, productBu = {}, stockingCertLevel = {}",configWithoutCert,productBu,stockingCertLevel);
    	if(configWithoutCert.isEmpty()){
            logger.info("stockingCertLevel is not null");
            if (productBu.equalsIgnoreCase("EBU")) {
                logger.info("productBu is EBU");
                logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
                return findMin(min,getRank("ebu_rank",stockingCertLevel));
            } else {
                logger.info("productBu is HHP");
                logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
                return findMin(min,getRank("hhp_rank",stockingCertLevel));
            }
        }
    	else {
    		logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
    		return null;
    	}
    }
}