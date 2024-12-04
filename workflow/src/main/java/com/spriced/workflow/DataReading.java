package com.spriced.workflow;

import flink.generic.db.Model.Condition;
import flink.generic.db.Service.GenericQueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Map;

@Service
public class DataReading {
    @Autowired
    GenericQueryService genServ;
    private static final Logger logger = LoggerFactory.getLogger(DataReading.class);

    public List<Map<String,Object>> read(String entityName, List<String> columns, List<Condition> conditions, String dbName) {
        logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
        logger.info("Parameter(entityName = {},columns = {},conditions = {},dbName = {})",entityName,columns.toString(),conditions.toString(),dbName);
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return genServ.read(entityName,columns, conditions, dbName);
    }
}
