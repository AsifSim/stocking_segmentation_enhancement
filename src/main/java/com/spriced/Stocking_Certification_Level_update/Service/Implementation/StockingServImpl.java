package com.spriced.Stocking_Certification_Level_update.Service.Implementation;

import com.spriced.Stocking_Certification_Level_update.DTO.Request.StockingRequest;
import com.spriced.Stocking_Certification_Level_update.DTO.Request.JsonRequest;
import com.spriced.Stocking_Certification_Level_update.DTO.Response.PartResponse;
import com.spriced.Stocking_Certification_Level_update.Model.Part;
import com.spriced.Stocking_Certification_Level_update.Repository.ConfigurationRepo;
import com.spriced.Stocking_Certification_Level_update.Repository.PStcokCertRepo;
import com.spriced.Stocking_Certification_Level_update.Repository.PartRepo;
import com.spriced.Stocking_Certification_Level_update.Repository.XcertificationLevelRepo;
import com.spriced.Stocking_Certification_Level_update.Service.StockingService;
import com.spriced.Stocking_Certification_Level_update.Transformer.DtoToModel.PartToPartResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class StockingServImpl implements StockingService {

    @Autowired
    ConfigurationRepo configRepo;

    @Autowired
    PartRepo partRepo;

    @Autowired
    XcertificationLevelRepo xcertRepo;

    @Autowired
    PStcokCertRepo pStockRepo;

    public PartResponse update(JsonRequest jsonReq){
        System.out.println("=============Inside update");
        List<String> configurations=new ArrayList<>();
        String partNumber="";
        for(StockingRequest reqBody:jsonReq.getData().values()){
            configurations.add(reqBody.getConfiguration());
            if(partNumber.isEmpty())partNumber=reqBody.getPart_number();
        }
        System.out.println("===========Inside the service and part_number="+partNumber);
//        Part part=partRepo.findByCode(reqBody.getPart_number()).get();
        Part part=partRepo.findByCode(partNumber).get();
        Integer certificationLevel=part.getCertificationLevel();
        System.out.println("certificationLevel============"+certificationLevel);
        String productBu=part.getProductBu();
        System.out.println("productBu==============="+productBu);
        String result="";

        //if configuration is empty in nrp_msbi_part_config
//        if(reqBody.getConfiguration().isEmpty()){
        if(configurations.isEmpty()){
            System.out.println("=========Inside the service in if");
            result=(xcertRepo.findById(certificationLevel)).get().getName();//Fetching certificationLevelName
            System.out.println("Inside if=========result "+result);
        }
        else{
            System.out.println("=========Inside the service in else");
//            List<String> configurations=reqBody.getConfiguration();//
            String configWithoutCert="";
            String stockingCertLevel="";
            Integer min=null;
            for(String config: configurations) {
                System.out.println("=========Inside the service in loop");
                stockingCertLevel = configRepo.findByCode(config).get().getStockingCertificationLevel();
                if (stockingCertLevel == null) {
                    System.out.println("==========in loop stockingCertLevel is null");
                    configWithoutCert += config + ",";
                } else {
                    if(configWithoutCert.isEmpty()){
                        System.out.println("==========in loop stockingCertLevel is not null");
                        if (productBu.equalsIgnoreCase("EBU")) {
                            System.out.println("==========productBu is EBU");
                            Integer rank = pStockRepo.findByCertification(stockingCertLevel).get().getEbuRank();
                            System.out.println("==========rank "+rank);
                            min=findMin(min,rank);
                            System.out.println("==========min "+min);
                        } else {
                            System.out.println("==========productBu is HHP");
                            Integer rank = pStockRepo.findByCertification(stockingCertLevel).get().getHhpRank();
                            System.out.println("==========rank "+rank);
                            min=findMin(min,rank);
                            System.out.println("==========min "+min);
                        }
                    }
                }
            }
                if(configWithoutCert.isEmpty()) result=""+min;
                else result="CHECK, "+configWithoutCert.substring(0,(configWithoutCert.length()-1));
            System.out.println("Inside else if=========result "+result);
        }
        return PartToPartResponse.PartToPartResponse(result,partNumber,"part","update");
    }

    public Integer findMin(Integer min, Integer rank){
        System.out.println("===========Inside findMin, min="+min+" rank="+rank);
        System.out.println("=============Going out of findMin");
        if(min==null)return rank;
        else return ((min<rank)?rank:min);
    }
}