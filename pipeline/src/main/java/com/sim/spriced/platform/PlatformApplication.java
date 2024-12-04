package com.sim.spriced.platform;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class PlatformApplication {

	private static final Logger logger = LoggerFactory.getLogger(PlatformApplication.class);

	public static void main(String[] args) {
		logger.info("{}.{} - Starting PlatformApplication...", PlatformApplication.class.getSimpleName(), "main");
		SpringApplication.run(PlatformApplication.class, args);
		logger.info("{}.{} - PlatformApplication started successfully.", PlatformApplication.class.getSimpleName(), "main");
	}
}


