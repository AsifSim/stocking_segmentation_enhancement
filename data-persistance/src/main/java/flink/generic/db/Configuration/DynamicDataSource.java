package flink.generic.db.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

public class DynamicDataSource extends AbstractRoutingDataSource {

    private static final ThreadLocal<String> contextHolder = new ThreadLocal<>();
    private static final Logger logger = LoggerFactory.getLogger(DynamicDataSource.class);


    @Override
    protected Object determineCurrentLookupKey() {
        logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return contextHolder.get();
    }

    public static void setDataSourceKey(String key) {
        logger.info("=============Inside Class DynamicDataSource, Function setDataSourceKey");
        logger.info("Parameter(key = {})",key);
        contextHolder.set(key);
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
    }

    public static void clearDataSourceKey() {
        logger.info("=============Inside Class DynamicDataSource, Function clearDataSourceKey");
        contextHolder.remove();
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
    }
}
