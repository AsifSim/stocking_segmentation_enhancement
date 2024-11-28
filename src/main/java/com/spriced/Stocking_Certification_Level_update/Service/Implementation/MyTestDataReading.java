//package com.spriced.Stocking_Certification_Level_update.Service.Implementation;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Service;
//
//import com.spriced.Stocking_Certification_Level_update.DTO.Request.JsonRequest;
//import com.spriced.Stocking_Certification_Level_update.DTO.Request.StockingRequest;
//import com.spriced.Stocking_Certification_Level_update.DTO.Response.PartResponse;
//import com.spriced.Stocking_Certification_Level_update.Service.StockingService;
//import com.spriced.Stocking_Certification_Level_update.Transformer.DtoToModel.PartToPartResponse;
//
//import flink.generic.db.Model.Condition;
//import flink.generic.db.Service.GenericQueryService;
//
//@Service
//public class MyTestDataReading implements StockingService {
//
//    @Autowired
//    GenericQueryService genServ;
//
//
//    public PartResponse update(JsonRequest jsonReq){
//        System.out.println("=============Inside Class StockingServImpl, Function update");
//        List<String> configurations=new ArrayList<>();
//        String partNumber="";
//        for(StockingRequest reqBody:jsonReq.getData().values()){
//        	String configValue=reqBody.getConfiguration();
//        	if(!configValue.isEmpty())configurations.add(reqBody.getConfiguration());
//            if(partNumber.isEmpty())partNumber=reqBody.getPart_number();
//        }
//        System.out.println("===========Inside the service and part_number="+partNumber);
//        
//        Condition condition=new Condition("code",partNumber,"=","AND");
////        condition.setColumn("code");
////        condition.setValue(partNumber);
////        condition.setOperator("=");
////        condition.setLogicalOperator("AND");
//        List<Condition> conditions=new ArrayList<>();
//        conditions.add(condition);
//        List<String> columns=new ArrayList<>();
//        columns.add("id");
//        columns.add("certification_level");
//        columns.add("code");
//        columns.add("suggested_stocking_cert");
//        columns.add("product_bu");
//        
////        List<String> columns=Arrays.asList("id","certification_level","code","suggested_stocking_cert","product_bu");
//        
//        List<Map<String,Object>> part=genServ.queryTable("part",columns,conditions,"spriced_meritor");
//        Long certificationLevel=(Long)(part.get(0)).get("certification_level");
//        System.out.println("certificationLevel============"+certificationLevel);
//        String productBu=(String)(part.get(0)).get("product_bu");
//        System.out.println("productBu==============="+productBu);
//        String result="";
//
//
//        System.out.println("=====configuration = "+configurations.toString());
//        if(configurations.isEmpty()){
//            System.out.println("=========Inside the service in if");
//            
//            condition=new Condition("id",certificationLevel,"=","AND");
////            condition.setColumn("id");
////            condition.setValue(certificationLevel);
////            condition.setOperator("=");
////            condition.setLogicalOperator("AND");
//            conditions.clear();
//            conditions.add(condition);
//            columns.clear();
//            columns.add("name");
//            
//            result=(String)genServ.queryTable("xcertification_level",columns,conditions,"spriced_meritor").get(0).get("name");
//            System.out.println("Inside if=========result "+result);
//        }
//        else{
//            System.out.println("=========Inside the service in else");
//            String configWithoutCert="";
//            String stockingCertLevel="";
//            Long min=null;
//            for(String config: configurations) {
//                System.out.println("=========Inside the service in loop");
//                
//                condition=new Condition("code",config,"=","AND");
////                condition.setColumn("code");
////                condition.setValue(config);
////                condition.setOperator("=");
////                condition.setLogicalOperator("AND");
//                conditions.clear();
//                conditions.add(condition);
//                columns.clear();
//                columns.add("stocking_certification_level");
//                
//                stockingCertLevel=(String)genServ.queryTable("configuration",columns,conditions,"spriced_meritor").get(0).get("stocking_certification_level");
//                if (stockingCertLevel == null) {
//                    System.out.println("==========in loop stockingCertLevel is null");
//                    configWithoutCert += config + ",";
//                } else {
//                    if(configWithoutCert.isEmpty()){
//                        System.out.println("==========in loop stockingCertLevel is not null");
//                        if (productBu.equalsIgnoreCase("EBU")) {
//                            System.out.println("==========productBu is EBU");
//                            
//                            condition=new Condition("certification",stockingCertLevel,"=","AND");
////                            condition.setColumn("certification");
////                            condition.setValue(stockingCertLevel);
////                            condition.setOperator("=");
////                            condition.setLogicalOperator("AND");
//                            conditions.clear();
//                            conditions.add(condition);
//                            columns.clear();
//                            columns.add("ebu_rank");
//                            
//                            Long rank = (Long)genServ.queryTable("p_stock_cert",columns,conditions,"spriced_meritor").get(0).get("ebu_rank");
//                            System.out.println("==========rank "+rank);
//                            min=findMin(min,rank);
//                            System.out.println("==========min "+min);
//                        } else {
//                            System.out.println("==========productBu is HHP");
//                            
//                            condition=new Condition("certification",stockingCertLevel,"=","AND");
////                            condition.setColumn("certification");
////                            condition.setValue(stockingCertLevel);
////                            condition.setOperator("=");
////                            condition.setLogicalOperator("AND");
//                            conditions.clear();
//                            conditions.add(condition);
//                            columns.clear();
//                            columns.add("hhp_rank");
//                            
//                            Long rank = (Long)genServ.queryTable("p_stock_cert",columns,conditions,"spriced_meritor").get(0).get("hhp_rank");
//                            
//                            System.out.println("==========rank "+rank);
//                            min=findMin(min,rank);
//                            System.out.println("==========min "+min);
//                        }
//                    }
//                }
//            }
//                if(configWithoutCert.isEmpty()) result=""+min;
//                else result="CHECK, "+configWithoutCert.substring(0,(configWithoutCert.length()-1));
//            System.out.println("Inside else if=========result "+result);
//        }
//        System.out.println("=============Going out of update");
//        return PartToPartResponse.PartToPartResponse(result,partNumber,"part","update");
//    }
//
//    public Long findMin(Long min, Long rank){
//        System.out.println("===========Inside Class StockingServImpl, function findMin, min="+min+" rank="+rank);
//        System.out.println("=============Going out of findMin");
//        if(min==null)return rank;
//        else return ((min<rank)?rank:min);
//    }
//}
