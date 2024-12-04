package com.spriced.workflow;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication(scanBasePackages = {"com.spriced.workflow", "flink.generic.db"})
@ComponentScan(basePackages = {"com.spriced.workflow", "flink.generic.db"})
public class WorkflowApplication {

	public static void main(String[] args) {
		SpringApplication.run(WorkflowApplication.class, args);
	}

}
