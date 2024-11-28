package com.spriced.Stocking_Certification_Level_update.Service.Implementation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.spriced.Stocking_Certification_Level_update.Exceptions.CurrentPartNumberNotFoundException;
import com.spriced.Stocking_Certification_Level_update.Exceptions.IdDoesntExistException;
import com.spriced.Stocking_Certification_Level_update.Exceptions.NoMatchingEccWithPartException;
import com.spriced.Stocking_Certification_Level_update.Exceptions.NoStockingCertForPartException;
import com.spriced.Stocking_Certification_Level_update.Exceptions.PartNotFoundException;
import com.spriced.Stocking_Certification_Level_update.Service.StockingSegmentCTTService;
import com.spriced.workflow.DataReading;

import flink.generic.db.Model.Condition;

@Service
public class StockingSegmentCTTServiceImpl implements StockingSegmentCTTService{
	
	@Value("${db_name}")
    private String DB_NAME;

    @Value("${PART}")
    private String PART;

    @Value("${CODE}")
    private String CODE;

    @Value("${CERTIFICATION_LEVEL}")
    private String CERTIFICATION_LEVEL;

    @Value("${ECC}")
    private String ECC;

    @Value("${BUSINESS_OWNER}")
    private String BUSINESS_OWNER;

    @Value("${ANNUAL_VOLUME}")
    private String ANNUAL_VOLUME;
    
    @Value("${STOCKING_CERT}")
    private String STOCKING_CERT;

    @Value("${CTT_STOCKING_SEGMENT}")
    private String CTT_STOCKING_SEGMENT;

    @Value("${P_STOCK_CES_MIN_SEGMENT}")
    private String P_STOCK_CES_MIN_SEGMENT;
    
    @Value("${MIN_PERCENTILE}")
    private String MIN_PERCENTILE;

    @Value("${MAX_PERCENTILE}")
    private String MAX_PERCENTILE;

    @Value("${STOCKING_SEGMENT}")
    private String STOCKING_SEGMENT;

    @Value("${P_STOCK_CTT_SEGMENT}")
    private String P_STOCK_CTT_SEGMENT;
    
    @Value("${ID}")
    private String ID;
    
    @Value("${XCERTIFICATION_LEVEL}")
    private String XCERTIFICATION_LEVEL;
    
    @Value("${CURRENT_PART_NUMBER}")
    private String CURRENT_PART_NUMBER;
    
    @Value("${INBOUND_STOCK_NRP_MSBI_SALES_VOLUME}")
    private String INBOUND_STOCK_NRP_MSBI_SALES_VOLUME;
	
	@Autowired
    DataReading genServ;
	private static final Logger logger = LoggerFactory.getLogger(StockingCertificationServImpl.class);

	@Override
	public String update(String partNumber,String productBU) throws PartNotFoundException, NoStockingCertForPartException, NoMatchingEccWithPartException, IdDoesntExistException, CurrentPartNumberNotFoundException {
		logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    	logger.info("Parameter(partNumber = {}, productBU = {})",partNumber,productBU);
	    productBU = !(productBU.isEmpty()) ? productBU.trim() : "";
	    String segmentInRange="";
	    Map<String,Object> part=getPartDetail(partNumber);
	    String stockingSegment = getCttStockSegment(partNumber, productBU,(Long)part.get(CERTIFICATION_LEVEL));
	    if(stockingSegment == null) {
	        	if((partNumber.endsWith("HX") || partNumber.endsWith("H")) && (part.get(BUSINESS_OWNER).toString().equalsIgnoreCase("CTT"))) stockingSegment=(part.get(ANNUAL_VOLUME)==null)?"D":"A";
	        	else if((segmentInRange=checkStockingInRange(part,partNumber))!=null)stockingSegment=segmentInRange;
	        	else stockingSegment="C";
	    }
	    logger.info("stockingSegment = {}",stockingSegment);
	    logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
	    return stockingSegment;
	    }
	
	public String checkStockingInRange(Map<String,Object> part,String partNumber) throws NoMatchingEccWithPartException, IdDoesntExistException, CurrentPartNumberNotFoundException {
		logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    	logger.info("Parameter(certification_level = {},ecc = {},business_owner = {},annual_volume = {})",part.get(CERTIFICATION_LEVEL),part.get(ECC),part.get(BUSINESS_OWNER),part.get(ANNUAL_VOLUME));
    	List<Condition> conditions = new ArrayList<>(List.of(new Condition(ECC, getCode((Long)part.get(ECC),ECC), "=", "AND")));
        List<String> columns = List.of(MIN_PERCENTILE,MAX_PERCENTILE,STOCKING_SEGMENT);
        logger.info("Going to execute read query from p_stock_ctt_segment entity");
        List<Map<String, Object>> stockCttSegmentList = genServ.read(P_STOCK_CTT_SEGMENT, columns, conditions, DB_NAME);
        logger.info("Read query from p_stock_ctt_segment entity has been executed");
        if(stockCttSegmentList.isEmpty()) throw new NoMatchingEccWithPartException("There is no ecc in p_stock_ctt_segment matching with part "+partNumber);
        logger.info("list = {}",stockCttSegmentList.toString());
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return findStockingSegment(getVolume((String)part.get(CURRENT_PART_NUMBER)),stockCttSegmentList);
	}
	
	public Long getVolume(String currentPartNumber) throws CurrentPartNumberNotFoundException {
		logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    	logger.info("Parameter(currentPartNumber = {})",currentPartNumber);
    	List<Condition> conditions = new ArrayList<>(List.of(new Condition(CURRENT_PART_NUMBER, currentPartNumber, "=", "AND")));
        List<String> columns = List.of("volume");
        logger.info("Going to execute read query from INBOUND_STOCK_NRP_MSBI_SALES_VOLUME entity");
        List<Map<String, Object>> volumeList = genServ.read(INBOUND_STOCK_NRP_MSBI_SALES_VOLUME, columns, conditions, DB_NAME);
        logger.info("Read query from INBOUND_STOCK_NRP_MSBI_SALES_VOLUME entity has been executed");
        if(volumeList.isEmpty()) throw new CurrentPartNumberNotFoundException("There is no current_part_number in INBOUND_STOCK_NRP_MSBI_SALES_VOLUME matching with part "+currentPartNumber);
        logger.info("list = {}",volumeList.toString());
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return (Long)volumeList.get(0).get("volume");
	}
	
	public String findStockingSegment(Long annualVolume, List<Map<String, Object>> stockCttSegmentList) {
		logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
    	logger.info("Parameter(annual_volume = {}, stockCttSegmentList = {})",annualVolume,stockCttSegmentList.toString());
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
    	return stockCttSegmentList.stream()
                .filter(stockCttSegment -> {
                    Long minPercentile = (Long) stockCttSegment.get("min_percentile");
                    Long maxPercentile = (Long) stockCttSegment.get("max_percentile");
                    logger.info("Parameter(min_percentile = {}, max_percentile = {},",minPercentile,maxPercentile);
                    return annualVolume >= minPercentile && annualVolume <= maxPercentile;
                })
                .map(stockCttSegment -> (String) stockCttSegment.get("stocking_segment")).findFirst().orElse(null);
    }

	    private String getCttStockSegment(String partNumber, String productBU,Long certification_level) throws NoStockingCertForPartException, IdDoesntExistException{
			logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
	    	logger.info("Parameter(partNumber = {}, productBU = {})",partNumber,productBU);
	        List<Condition> conditions = new ArrayList<>(List.of(new Condition(STOCKING_CERT, getCode(certification_level,XCERTIFICATION_LEVEL), "=", "AND")));
	        logger.info("code from X_CERTIFICATION_LEVEL = {}",conditions.get(0).getValue());
	        List<String> columns = List.of(CTT_STOCKING_SEGMENT);
	        logger.info("Going to execute read query from p_stock_ces_min_segment entity");
	        List<Map<String, Object>> stockCesMinSegmentList = genServ.read(P_STOCK_CES_MIN_SEGMENT, columns, conditions, DB_NAME);
	        if(stockCesMinSegmentList.isEmpty()) throw new NoStockingCertForPartException("Stocking_Cert for the part "+partNumber+" doesn't exist");
	        String cttStockingSeg= (String)stockCesMinSegmentList.get(0).get(CTT_STOCKING_SEGMENT);
	        logger.info("Read query from p_stock_ces_min_segment entity has been executed");
	        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
	        return cttStockingSeg;
	    }
	    
	    public String getCode(Long entityId, String name) throws IdDoesntExistException {
			logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
	    	logger.info("Parameter(Id = {})",entityId);
	    	List<Condition> conditions = new ArrayList<>(List.of(new Condition(ID, entityId, "=", "AND")));
	        List<String> columns = List.of(CODE);
	        logger.info("Going to execute read query from p_stock_ces_min_segment entity");
	        List<Map<String, Object>> fetchList = genServ.read(name, columns, conditions, DB_NAME);
	        if(fetchList.isEmpty()) throw new IdDoesntExistException("ID "+entityId+" does not exist for entity "+name);
	        String CorrespondingCode= (String)fetchList.get(0).get(CODE);
	        logger.info("Read query from p_stock_ces_min_segment entity has been executed");
	        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
	        return CorrespondingCode;
	    }
	    
	    public Map<String, Object> getPartDetail(String partNumber) throws PartNotFoundException {
			logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
	    	logger.info("Parameter(partNumber = {})",partNumber);
	    	List<Condition> conditions = new ArrayList<>(List.of((new Condition(CODE, partNumber, "=", "AND"))));
	        List<String> columns = List.of(CERTIFICATION_LEVEL,ECC,BUSINESS_OWNER,ANNUAL_VOLUME,CURRENT_PART_NUMBER);
	        logger.info("Going to execute read query from part entity");
	        List<Map<String, Object>> partDetailList = genServ.read(PART, columns, conditions, DB_NAME);
	        if(partDetailList.isEmpty()) throw new PartNotFoundException("partNumber = "+partNumber+ " doesn't exist");
	        Map<String, Object> partDetail=partDetailList.get(0);
	        logger.info("Read query from part entity has been executed");
	        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
	        return partDetail;
	    }
	}
