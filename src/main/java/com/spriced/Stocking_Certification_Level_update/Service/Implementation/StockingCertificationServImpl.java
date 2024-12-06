package com.spriced.Stocking_Certification_Level_update.Service.Implementation;

import com.spriced.Stocking_Certification_Level_update.Exceptions.CertificationNotFoundException;
import com.spriced.Stocking_Certification_Level_update.Exceptions.InvaliCertificationException;
import com.spriced.Stocking_Certification_Level_update.Exceptions.PartNotFoundException;
import com.spriced.Stocking_Certification_Level_update.Service.StockingCertificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import flink.generic.db.Model.Condition;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import com.spriced.workflow.*;

@Service
public class StockingCertificationServImpl implements StockingCertificationService {

    @Value("${EQUALS}")
    private String EQUALS;

    @Value("${AND}")
    private String AND;

    @Value("${NRP_MSBI_PART_ALL_CONFIGS}")
    private String NRP_MSBI_PART_ALL_CONFIGS;

    @Value("${PART_NUMBER}")
    private String PART_NUMBER;

    @Value("${CONFIGURATION}")
    private String CONFIGURATION;

    @Value("${db_name}")
    private String DB_NAME;

    @Value("${CERTIFICATION}")
    private String CERTIFICATION;

    @Value("${ID}")
    private String ID;

    @Value("${NAME}")
    private String NAME;

    @Value("${P_STOCK_CERT}")
    private String P_STOCK_CERT;

    @Value("${XCERTIFICATION_LEVEL}")
    private String XCERTIFICATION_LEVEL;

    @Value("${CODE}")
    private String CODE;

    @Value("${STOCKING_CERTIFICATION_LEVEL}")
    private String STOCKING_CERTIFICATION_LEVEL;

    @Value("${EBU}")
    private String EBU;

    @Value("${PRODUCT_BU}")
    private String PRODUCT_BU;

    @Value("${CERTIFICATION_LEVEL}")
    private String CERTIFICATION_LEVEL;

    @Value("${PART}")
    private String PART;

    @Value("${EBU_RANK}")
    private String EBU_RANK;

    @Value("${HHP_RANK}")
    private String HHP_RANK;

    @Value("${SUGGESTED_STOCKING_CERT}")
    private String SUGGESTED_STOCKING_CERT;
    
    @Autowired
    DataReading genServ;
    private static final Logger logger = LoggerFactory.getLogger(StockingCertificationServImpl.class);

    public String update(String partNumber) throws PartNotFoundException {
        logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
        logger.info("Parameter(partNumber = {})",partNumber);
        List<String> configurations=new ArrayList<>();
        List<Condition> conditions = new ArrayList<>(List.of(new Condition(PART_NUMBER,partNumber,EQUALS,AND)));
        List<String> columns = new ArrayList<>(List.of(CONFIGURATION));
        logger.info("Going to execute read query from nrp_msbi_part_all_configs entity");
        List<Map<String,Object>> response=genServ.read(NRP_MSBI_PART_ALL_CONFIGS,columns,conditions,DB_NAME);
        logger.info("Read query from nrp_msbi_part_all_configs entity has been executed");
        if(response.isEmpty()) throw new PartNotFoundException("partNumber = "+partNumber+ " doesn't exist in "+NRP_MSBI_PART_ALL_CONFIGS);
        logger.info("configurations = {}",configurations.toString());
        configurations.addAll(response.stream().map(map -> (String) map.get(CONFIGURATION)).collect(Collectors.toList()));
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
    	List<Condition> conditions = new ArrayList<>(List.of(new Condition(CERTIFICATION,stockingCertLevel,EQUALS,AND)));
    	List<String> columns = new ArrayList<>(List.of(columnName));
        logger.info("Going to execute read query from p_stock_cert entity");
        List<Map<String,Object>> pStockList=genServ.read(P_STOCK_CERT,columns,conditions,DB_NAME);
        if(pStockList.isEmpty()) throw new RuntimeException("certification "+stockingCertLevel+" doesn't exist");
        Long rank = (Long)pStockList.get(0).get(columnName);
        logger.info("Read query from p_stock_cert entity has been executed");
        logger.info("rank {}",rank);
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return rank;
    }
    
    public String resultWhenNoConfiguration(Long certificationLevel) throws InvaliCertificationException{
    	logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    	logger.info("Parameter(certificationLevel = {})",certificationLevel);
        List<Condition> conditions = new ArrayList<>(List.of(new Condition(ID,certificationLevel,EQUALS,AND)));        
        List<String> columns = new ArrayList<>(List.of(NAME));
        logger.info("Going to execute read query from xcertification_level entity");
        List<Map<String,Object>> xCertList=genServ.read(XCERTIFICATION_LEVEL,columns,conditions,DB_NAME);
        if(xCertList.isEmpty()) throw new InvaliCertificationException("There is no certification_level "+certificationLevel+" with id = "+ID);
        String result=(String)xCertList.get(0).get(NAME);
        logger.info("Read query from xcertification_level entity has been executed");
        logger.info("result = {}",result);
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return result;
    }
    
    public String getResult(List<String> configurations,String productBu) {
    	logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    	logger.info("Parameter(configurations = {}, productBu = {})",configurations.toString(),productBu);    	
    	AtomicReference<Long> min = new AtomicReference<>(null);
        StringBuilder configWithoutCert = new StringBuilder();
        configurations.stream()
            .peek(config -> logger.info("Inside the service in loop"))
            .forEach(config -> {
                List<Condition> conditions = new ArrayList<>(List.of(new Condition(CODE, config, EQUALS, AND)));
                List<String> columns = new ArrayList<>(List.of(STOCKING_CERTIFICATION_LEVEL));
                logger.info("Going to execute read query from configuration entity");
                List<Map<String,Object>> configList = genServ.read(CONFIGURATION, columns, conditions, DB_NAME);
                logger.info("Read query from configuration entity has been executed");
                if(configList.isEmpty())throw new RuntimeException("configuration "+CODE+" doesn't exist");
                String stockingCertLevel = (String) configList.get(0).get(STOCKING_CERTIFICATION_LEVEL);
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
    
    public String getPartResponse(String partNumber,List<String> configurations) throws PartNotFoundException {
    	logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    	logger.info("partNumber = {}, configurations = {}",partNumber,configurations.toString());
    	List<Condition> conditions = new ArrayList<>(List.of(new Condition(CODE, partNumber, EQUALS, AND)));
        List<String> columns = new ArrayList<>(List.of(ID, CERTIFICATION_LEVEL, CODE, SUGGESTED_STOCKING_CERT, PRODUCT_BU));
        logger.info("Going to execute read query from part entity");
        List<Map<String,Object>> part=genServ.read(PART,columns,conditions,DB_NAME);
        logger.info("Read query from part entity has been executed");
        if(part.isEmpty()) throw new PartNotFoundException("partNumber = "+partNumber+ " doesn't exist");
        Long certificationLevel=(Long)(part.get(0)).get(CERTIFICATION_LEVEL);
        logger.info("certificationLevel = {}",certificationLevel);
        String productBu=(String)(part.get(0)).get(PRODUCT_BU);
        logger.info("productBu = {}",productBu);
        String result=configurations.isEmpty()?resultWhenNoConfiguration(certificationLevel):getResult(configurations,productBu);
        logger.info("result = {}",result);
        logger.info("=============Going out of update");
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return result;
    }
    
    public Long getMin(String configWithoutCert, String productBu, String stockingCertLevel, Long min) {
    	logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    	logger.info("configWithoutCert = {}, productBu = {}, stockingCertLevel = {}",configWithoutCert,productBu,stockingCertLevel);
    	if(configWithoutCert.isEmpty()){
            logger.info("stockingCertLevel is not null");
            if (productBu.equalsIgnoreCase(EBU)) {
                logger.info("productBu is EBU");
                logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
                return findMin(min,getRank(EBU_RANK,stockingCertLevel));
            } else {
                logger.info("productBu is HHP");
                logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
                return findMin(min,getRank(HHP_RANK,stockingCertLevel));
            }
        }
    	else {
    		logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
    		return null;
    	}
    }
}