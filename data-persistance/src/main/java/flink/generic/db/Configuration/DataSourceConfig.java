package flink.generic.db.Configuration;

import flink.generic.db.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class DataSourceConfig {
    private static final Logger logger = LoggerFactory.getLogger(DataSourceConfig.class);

    @Bean
    public DataSource dataSource() {
        logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
        DynamicDataSource dataSourceConfig = new DynamicDataSource();
        logger.info("Setting up DB Connection");
        DriverManagerDataSource dataSource1 = getDataSource();
        Map<Object, Object> dataSourceMap = new HashMap<>();
        dataSourceMap.put(Constants.DB_NAME, dataSource1);
        dataSourceConfig.setTargetDataSources(dataSourceMap);
        dataSourceConfig.setDefaultTargetDataSource(dataSource1);
        logger.info("DB Connection is done");
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return dataSourceConfig;
    }

    public DriverManagerDataSource getDataSource(){
        logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
        DriverManagerDataSource dataSource1 = new DriverManagerDataSource();
        dataSource1.setDriverClassName(Constants.DRIVER);
        dataSource1.setUrl(Constants.DB_URL);
        dataSource1.setUsername(Constants.DB_USERNAME);
        dataSource1.setPassword(Constants.DB_PASSWORD);
        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return dataSource1;
    }
}

