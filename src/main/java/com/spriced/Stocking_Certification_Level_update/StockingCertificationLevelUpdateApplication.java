package com.spriced.Stocking_Certification_Level_update;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.PropertySource;

@SpringBootApplication(scanBasePackages = {"com.spriced.Stocking_Certification_Level_update", "flink.generic.db","com.example.demo","com.spriced.workflow"})
@ComponentScan(basePackages = {"com.spriced.Stocking_Certification_Level_update", "flink.generic.db","com.example.demo","com.spriced.workflow"})
@PropertySource({"classpath:StockingSegmentCTTServiceImpl.properties", "classpath:application.properties"})
public class StockingCertificationLevelUpdateApplication {

	public static void main(String[] args) {
		SpringApplication.run(StockingCertificationLevelUpdateApplication.class, args);
	}

}
