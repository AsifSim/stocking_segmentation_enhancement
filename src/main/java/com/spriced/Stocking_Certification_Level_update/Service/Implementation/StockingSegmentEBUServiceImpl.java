package com.spriced.Stocking_Certification_Level_update.Service.Implementation;

import com.spriced.workflow.DataReading;
import flink.generic.db.Model.Condition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Service
public class StockingSegmentEBUServiceImpl {

	@Value("${EQUALS}")
	private String EQUALS;

	@Value("${AND}")
	private String AND;

	@Value("${db_name}")
	private String DB_NAME;

	@Value("${PART}")
	private String PART; //PART TABLE

	@Value("${CODE}")
	private String CODE; // CODE COLUMN IN PART TABLE

	@Value("${CURRENT_PART_NUMBER}")
	private String CURRENT_PART_NUMBER; //  CPN COLUMN IN PART TABLE

	@Value("${PRODUCT_BU}")
	private String PRODUCT_BU;

	@Value("${SUGGESTED_STOCKING_CERT}")
	private String SUGGESTED_STOCKING_CERT;

	@Value("${ECC}")
	private String ECC;

	@Value("${STOCKING_CERT}")
	private String STOCKING_CERT;

	@Value("${EBU_MIN_STOCKING_SEGMENT}")
	private String EBU_MIN_STOCKING_SEGMENT;

	@Value("${X_STOCK_CES_MIN_SEGMENT}")
	private String X_STOCK_CES_MIN_SEGMENT;

	@Value("${INBOUND_STOCK_NRP_MSBI_SALES_VOLUME}")
	private String INBOUND_STOCK_NRP_MSBI_SALES_VOLUME;

	@Value("${VOLUME}")
	private String VOLUME;

	@Value("${SALES}")
	private String SALES;

	@Value("${P_STOCK_COMPETITIVENESS_SCORE}")
	private String P_STOCK_COMPETITIVENESS_SCORE;

	@Value("${SCORE}")
	private String SCORE;

	@Value("${CERTIFICATION}")
	private String CERTIFICATION;

	@Value("${RESULTING_GROSS_MARGIN}")
	private String RESULTING_GROSS_MARGIN;
	
	@Value("${P_STOCK_CES_MIN_SEGMENT}")
	private String P_STOCK_CES_MIN_SEGMENT;

	@Value("${CUSTOMER_EXPECTATION_SCORE}")
	private String CUSTOMER_EXPECTATION_SCORE;

	@Value("${QUALITY_SCORE}")
	private String QUALITY_SCORE;

	@Value("${P_STOCK_QUALITY_SCORE}")
	private String P_STOCK_QUALITY_SCORE;

	@Value("${EBU_STOCKING_SEGMENT}")
	private String EBU_STOCKING_SEGMENT;

	private static final Logger logger = LoggerFactory.getLogger(StockingSegmentEBUServiceImpl.class);
	private final DataReading genServ;

	public StockingSegmentEBUServiceImpl(DataReading genServ) {
		logger.info("Initializing StockingSegmentEBUServiceImpl with genServ dependency.");
		this.genServ = genServ;
	}

	public String update(String partNumber) {
		logger.info("Starting the process to find the stocking segment for partNumber: {}", partNumber);
		return executeSafely(() -> {
			logger.info("Fetching part details for partNumber: {}", partNumber);
			Map<String, Object> part = validateAndFetchPart(partNumber);
			logger.info("Fetched part details: {}", part);

			logger.info("Validating Product BU for partNumber: {}", partNumber);
			validateProductBU((String) part.get(PRODUCT_BU));
			logger.info("Product BU validated for partNumber: {}", partNumber);

			logger.info("Calculating Quality Score for partNumber: {}", partNumber);
			int qualityScore = calculateQualityScore(partNumber);
			logger.info("Calculated Quality Score: {}", qualityScore);

			logger.info("Fetching Quality Score Segment for Quality Score: {}", qualityScore);
			String qualityScoreSegment = getQualityScoreSegment(qualityScore);
			logger.info("Fetched Quality Score Segment: {}", qualityScoreSegment);

			logger.info("Determining final stocking segment for partNumber: {}", partNumber);
			String finalSegment = determineFinalSegment(partNumber, qualityScoreSegment);
			logger.info("Final stocking segment for partNumber {}: {}", partNumber, finalSegment);

			return finalSegment;
		});
	}

	private <T> T executeSafely(Supplier<T> action) {
		logger.info("Executing safely with provided action: {}", action.getClass().getSimpleName());
		try {
			T result = action.get();
			logger.info("Action executed successfully with result: {}", result);
			return result;
		} catch (Exception e) {
			logger.error("An error occurred while executing action: {}", e.getMessage(), e);
			throw new RuntimeException("Operation failed due to an unexpected error.", e);
		}
	}


	private Map<String, Object> validateAndFetchPart(String partNumber) {
		logger.info("Starting validation and fetching part details for partNumber: {}", partNumber);

		// Fetch the part details
		logger.info("Fetching part details for partNumber: {}", partNumber);
		List<Map<String, Object>> partResult = fetchPartDetails(partNumber);

		// Check if the result is empty
		if (partResult.isEmpty()) {
			logger.warn("Part details not found for partNumber: {}", partNumber);
			throw new IllegalArgumentException("Part not found for partNumber: " + partNumber);
		}

		// Fetch the first result and log
		Map<String, Object> part = partResult.get(0);
		logger.info("Fetched part details for partNumber: {}: {}", partNumber, part);

		return part;
	}


	private List<Map<String, Object>> fetchPartDetails(String partNumber) {
		logger.info("Starting to fetch part details for partNumber: {}", partNumber);

		return executeSafely(() -> {
			logger.info("Executing query to fetch details from '{}' table with columns [{}] and condition [code = {}]",
					PART,
					List.of(PRODUCT_BU, CODE, SUGGESTED_STOCKING_CERT, ECC),
					partNumber);

			List<Map<String, Object>> result = genServ.read(
					PART,
					List.of(PRODUCT_BU, CODE, SUGGESTED_STOCKING_CERT, ECC),
					List.of(new Condition(CODE, partNumber, EQUALS, AND)),
					DB_NAME
			);

			logger.info("Query executed successfully. Retrieved {} record(s) for partNumber: {}", result.size(), partNumber);
			return result;
		});
	}


	// Step 2: Validate Product BU
	private void validateProductBU(String productBU) {
		logger.info("Validating Product BU: {}", productBU);

		productBU = trimProductBU(productBU);
		logger.info("Trimmed Product BU: {}", productBU);

		if (!"EBU".equalsIgnoreCase(productBU)) {
			logger.warn("Validation failed. Product BU is not 'EBU'. Found: {}", productBU);
			throw new IllegalArgumentException("Invalid Product BU. Expected 'EBU'.");
		}

		logger.info("Product BU validated successfully as 'EBU'.");
	}


	// Step 3: Calculate Quality Score
	private int calculateQualityScore(String partNumber) {
		logger.info("Starting Quality Score calculation for partNumber: {}", partNumber);

		logger.info("Calculating Profit Score for partNumber: {}", partNumber);
		int profitScore = calculateProfitScore(partNumber);
		logger.info("Calculated Profit Score: {}", profitScore);

		logger.info("Calculating Expectation Score for partNumber: {}", partNumber);
		int expectationScore = calculateExpectationScore(partNumber);
		logger.info("Calculated Expectation Score: {}", expectationScore);

		logger.info("Calculating Competitiveness Score for partNumber: {}", partNumber);
		int competitivenessScore = calculateCompetitivenessScore(partNumber);
		logger.info("Calculated Competitiveness Score: {}", competitivenessScore);

		int qualityScore = (2 * profitScore) + expectationScore + competitivenessScore;
		logger.info("Final Quality Score for partNumber {}: {}", partNumber, qualityScore);

		return qualityScore;
	}


	// Step 4: Determine Final Segment
	private String determineFinalSegment(String partNumber, String qualityScoreSegment) {
		logger.info("Starting to determine the final segment for partNumber: {}", partNumber);

		logger.info("Fetching minimum segment from certification for partNumber: {}", partNumber);
		String minSegmentFromCertification = fetchEbuMinStockingSegment(partNumber);
		logger.info("Fetched minimum segment from certification: {}", minSegmentFromCertification);

		logger.info("Comparing and updating EBU Stocking Segment from top 80% sales for partNumber: {}", partNumber);
		String minSegmentFrom80Percent = compareAndUpdateEbuStockingSegmentFrom80PerSales(partNumber, qualityScoreSegment);
		logger.info("Determined minimum segment from top 80% sales: {}", minSegmentFrom80Percent);

		logger.info("Comparing and updating EBU Stocking Segment from top 2000 sales for partNumber: {}", partNumber);
		String minSegmentFrom2000Sales = compareAndUpdateEbuStockingSegmentFrom2000Sales(partNumber, qualityScoreSegment);
		logger.info("Determined minimum segment from top 2000 sales: {}", minSegmentFrom2000Sales);

		logger.info("Comparing all segments to determine the final stocking segment.");
		String finalSegment = compareStockingSegments(
				minSegmentFrom2000Sales, minSegmentFrom80Percent, qualityScoreSegment, minSegmentFromCertification);
		logger.info("Final stocking segment for partNumber {}: {}", partNumber, finalSegment);

		return finalSegment;
	}


	public String compareStockingSegments(String... segments) {
		logger.info("Starting comparison of stocking segments: {}", Arrays.toString(segments));

		if (segments == null || segments.length == 0) {
			logger.error("No segments provided for comparison.");
			return "Invalid comparison values";
		}

		String finalSegment = Arrays.stream(segments)
				.filter(Objects::nonNull)
				.peek(segment -> logger.info("Processing segment: {}", segment))
				.max(Comparator.comparingInt(this::getSegmentIndex))
				.orElse(null);

		if (finalSegment == null) {
			logger.error("All provided segments are either null or invalid.");
			return "Invalid comparison values";
		}

		logger.info("Determined the highest-ranking segment: {}", finalSegment);
		return finalSegment;
	}



	public String fetchEbuMinStockingSegment(String partNumber) {
		logger.info("Starting to fetch EBU Min Stocking Segment for partNumber: {}", partNumber);

		return executeSafely(() -> {
			Optional<String> stockingCertOpt = fetchStockingCert(partNumber);

			if (stockingCertOpt.isEmpty()) {
				logger.warn("No Stocking Certification found for partNumber: {}", partNumber);
				return "No EBU Min Stocking Segment found";
			}

			return fetchMinStockingSegment(stockingCertOpt.get())
					.orElseGet(() -> {
						logger.warn("No EBU Min Stocking Segment found for Stocking Certification: {}", stockingCertOpt.get());
						return "No EBU Min Stocking Segment found";
					});
		});
	}




	private Optional<String> fetchMinStockingSegment(String stockingCert) {
		logger.info("Fetching EBU Min Stocking Segment for Stocking Certification: {}", stockingCert);

		return genServ.read(
						X_STOCK_CES_MIN_SEGMENT,
						List.of(EBU_MIN_STOCKING_SEGMENT),
						List.of(new Condition(STOCKING_CERT, stockingCert, EQUALS, AND)),
						DB_NAME
				).stream()
				.findFirst()
				.map(record -> {
					String segment = (String) record.get(EBU_MIN_STOCKING_SEGMENT);
					if (segment != null) {
						logger.info("Found EBU Min Stocking Segment: {} for Stocking Certification: {}", segment, stockingCert);
					} else {
						logger.warn("No EBU Min Stocking Segment found for Stocking Certification: {}", stockingCert);
					}
					return segment;
				});
	}



	public String compareAndUpdateEbuStockingSegment(String qualityScoreSegment, String minStockingSegmentFromCertification) {
		logger.info("Comparing Quality Score Segment: {} with Min Stocking Segment from Certification: {}", qualityScoreSegment, minStockingSegmentFromCertification);

		if (!areSegmentsValid(qualityScoreSegment, minStockingSegmentFromCertification)) {
			logger.error("Invalid segments provided for comparison. QualityScoreSegment: {}, MinStockingSegmentFromCertification: {}", qualityScoreSegment, minStockingSegmentFromCertification);
			return "Invalid comparison values";
		}

		String updatedSegment = updateSegmentIfNecessary(qualityScoreSegment, minStockingSegmentFromCertification);
		logger.info("Updated Min Stocking Segment: {}", updatedSegment);
		return updatedSegment;
	}


	private boolean areSegmentsValid(String qualityScoreSegment, String minStockingSegmentFromCertification) {
		logger.info("Validating segments: QualityScoreSegment: {}, MinStockingSegmentFromCertification: {}",
				qualityScoreSegment, minStockingSegmentFromCertification);

		boolean isValid = qualityScoreSegment != null && minStockingSegmentFromCertification != null;

		if (!isValid) {
			logger.error("Segment validation failed. One or both segments are null.");
		} else {
			logger.info("Segments are valid for comparison.");
		}

		return isValid;
	}


	private String updateSegmentIfNecessary(String qualityScoreSegment, String minStockingSegmentFromCertification) {
		logger.info("Starting comparison of segments. QualityScoreSegment: {}, MinStockingSegmentFromCertification: {}",
				qualityScoreSegment, minStockingSegmentFromCertification);

		int qualityIndex = getSegmentIndex(qualityScoreSegment);
		int minIndex = getSegmentIndex(minStockingSegmentFromCertification);

		logger.info("QualityScoreSegment index: {}, MinStockingSegmentFromCertification index: {}", qualityIndex, minIndex);

		String updatedSegment = minIndex < qualityIndex ? qualityScoreSegment : minStockingSegmentFromCertification;

		logger.info("Comparison result: Selected Segment: {}", updatedSegment);
		return updatedSegment;
	}


	private int getSegmentIndex(String segment) {
		logger.info("Retrieving index for segment: {}", segment);

		int index = switch (segment) {
			case "A" -> 3;
			case "B" -> 2;
			case "C" -> 1;
			case "D" -> 0;
			default -> {
				logger.warn("Invalid segment: {}. Returning default index -1.", segment);
				yield -1;
			}
		};

		logger.info("Segment: {}, Index: {}", segment, index);
		return index;
	}




	public String compareAndUpdateEbuStockingSegmentFrom80PerSales(String partNumber, String qualityScoreSegment) {
		logger.info("Starting comparison and update of EBU Stocking Segment from 80% sales for partNumber: {}", partNumber);

		return fetchCurrentPartNumber(partNumber)
				.flatMap(currentPartNumber -> {
					logger.info("Fetched currentPartNumber: {}", currentPartNumber);

					return fetchEccValue(currentPartNumber)
							.flatMap(eccValue -> {
								logger.info("Fetched ECC value for currentPartNumber {}: {}", currentPartNumber, eccValue);

								boolean isTop80 = isInTop80Percent(currentPartNumber, eccValue);
								logger.info("Is currentPartNumber {} in top 80% of sales for ECC {}: {}", currentPartNumber, eccValue, isTop80);

								return isTop80
										? Optional.of("B")
										: Optional.of(qualityScoreSegment);
							});
				})
				.orElseThrow(() -> {
					logger.error("Invalid partNumber or ECC value for partNumber: {}", partNumber);
					return new IllegalArgumentException("Invalid part number or ECC value.");
				});
	}


	private Optional<String> fetchCurrentPartNumber(String partNumber) {
		logger.info("Fetching currentPartNumber for partNumber: {}", partNumber);
		return executeSafely(() -> genServ.read(PART, List.of(CURRENT_PART_NUMBER),
						List.of(new Condition(CODE, partNumber, EQUALS, AND)), DB_NAME)
				.stream()
				.findFirst()
				.map(record -> {
					String currentPartNumber = (String) record.get(CURRENT_PART_NUMBER);
					logger.info("Fetched currentPartNumber: {} for partNumber: {}", currentPartNumber, partNumber);
					return currentPartNumber;
				}));
	}
	
	private Optional<String> fetchEccValue(String currentPartNumber) {
		logger.info("Fetching ECC value for currentPartNumber: {}", currentPartNumber);
		return executeSafely(() -> genServ.read(INBOUND_STOCK_NRP_MSBI_SALES_VOLUME, List.of(ECC),
						List.of(new Condition(CURRENT_PART_NUMBER, currentPartNumber, EQUALS, AND)), DB_NAME)
				.stream()
				.findFirst()
				.map(record -> {
					String eccValue = (String) record.get(ECC);
					logger.info("Fetched ECC value: {} for currentPartNumber: {}", eccValue, currentPartNumber);
					return eccValue;
				}));
	}


	private boolean isInTop80Percent(String currentPartNumber, String eccValue) {
		logger.info("Checking top 80% for currentPartNumber: {} and ECC: {}", currentPartNumber, eccValue);

		List<Map<String, Object>> sortedSales = genServ.read(
						INBOUND_STOCK_NRP_MSBI_SALES_VOLUME,
						List.of(CURRENT_PART_NUMBER, VOLUME),
						List.of(new Condition(ECC, eccValue, EQUALS, AND)),
						DB_NAME
				).stream()
				.sorted((m1, m2) -> Double.compare(((Number) m2.get(VOLUME)).doubleValue(), ((Number) m1.get("volume")).doubleValue()))
				.toList();

		boolean isInTop80Percent = sortedSales.stream()
				.limit((int) Math.ceil(sortedSales.size() * 0.8))
				.anyMatch(record -> currentPartNumber.equals(record.get(CURRENT_PART_NUMBER)));

		logger.info("Is currentPartNumber: {} in top 80%: {}", currentPartNumber, isInTop80Percent);
		return isInTop80Percent;
	}



	public String compareAndUpdateEbuStockingSegmentFrom2000Sales(String partNumber, String qualityScoreSegment) {
		logger.info("Comparing and updating EBU Stocking Segment for partNumber: {}", partNumber);

		String currentPartNumber = fetchCurrentPartNumber(partNumber)
				.orElseThrow(() -> {
					logger.error("Current part number not found for part number: {}", partNumber);
					return new IllegalArgumentException("Current part number not found for part number: " + partNumber);
				});

		List<Map<String, Object>> top2000Sales = genServ.read(INBOUND_STOCK_NRP_MSBI_SALES_VOLUME,
						List.of(CURRENT_PART_NUMBER, SALES), new ArrayList<>(), DB_NAME).stream()
				.sorted((map1, map2) -> Double.compare(
						((Number) map2.getOrDefault(SALES, 0)).doubleValue(),
						((Number) map1.getOrDefault(SALES, 0)).doubleValue()))
				.limit(2000)
				.collect(Collectors.toList());

		boolean isInTop2000 = isInTop2000(currentPartNumber, top2000Sales);
		logger.info("Is currentPartNumber: {} in top 2000 sales: {}", currentPartNumber, isInTop2000);

		return isInTop2000 ? "B" : qualityScoreSegment;
	}



	private boolean isInTop2000(String currentPartNumber, List<Map<String, Object>> top2000Sales) {
		logger.info("Checking if currentPartNumber: {} is in top 2000 sales list.", currentPartNumber);
		boolean result = top2000Sales.stream()
				.anyMatch(item -> currentPartNumber.equals(item.get(CURRENT_PART_NUMBER)));
		logger.info("Result for currentPartNumber: {} in top 2000 sales: {}", currentPartNumber, result);
		return result;
	}




	public String getQualityScoreSegment(int qualityScore) {
		logger.info("Fetching Quality Score Segment for Quality Score: {}", qualityScore);
		return executeSafely(() -> {
			Optional<String> segment = fetchQualityScoreSegment(qualityScore);
			if (segment.isEmpty()) {
				logger.warn("No matching Quality Score Segment found for Quality Score: {}", qualityScore);
			} else {
				logger.info("Fetched Quality Score Segment: {} for Quality Score: {}", segment.get(), qualityScore);
			}
			return segment.orElse("No matching segment found");
		});
	}


	private Optional<String> fetchQualityScoreSegment(int qualityScore) {
		logger.info("Executing query to fetch Quality Score Segment for Quality Score: {}", qualityScore);

		List<Map<String, Object>> result = genServ.read(
				P_STOCK_QUALITY_SCORE,
				List.of(EBU_STOCKING_SEGMENT),
				List.of(new Condition(QUALITY_SCORE, qualityScore, EQUALS, AND)),
				DB_NAME
		);

		logger.info("Query executed. Result size: {}", result.size());
		if (result.isEmpty()) {
			logger.warn("No records found for Quality Score: {}", qualityScore);
		} else {
			logger.info("Fetched record: {}", result.get(0));
		}

		return result.stream()
				.findFirst()
				.map(record -> {
					String segment = (String) record.get(EBU_STOCKING_SEGMENT);
					logger.info("Extracted EBU Stocking Segment: {}", segment);
					return segment;
				});
	}

	private String trimProductBU(String productBU) {
		logger.info("Trimming Product BU: {}", productBU);
		String trimmedProductBU = productBU != null ? productBU.trim() : "";
		logger.info("Trimmed Product BU: {}", trimmedProductBU);
		return trimmedProductBU;
	}

	private int calculateProfitScore(String partNumber) {
		logger.info("Calculating Profit Score for partNumber: {}", partNumber);
		return executeSafely(() -> {
			String currentPartNumber = getCurrentPartNumber(partNumber);
			logger.info("Fetched currentPartNumber: {}", currentPartNumber);

			if (isInTop2000(currentPartNumber)) {
				logger.info("PartNumber {} is in the top 2000. Returning Profit Score: 3", partNumber);
				return 3;
			}

			String combination = getEccStockingCertCombination(partNumber);
			logger.info("Fetched ECC and StockingCert combination: {}", combination);

			int profitScore = isInTop100(combination) ? 2 : 1;
			logger.info("PartNumber {} is inTop100: {}. Returning Profit Score: {}", partNumber, profitScore == 2, profitScore);
			return profitScore;
		});
	}

	private String getCurrentPartNumber(String partNumber) {
		logger.info("Fetching current part number for partNumber: {}", partNumber);
		return executeSafely(() ->
				genServ.read(PART, List.of(CURRENT_PART_NUMBER), List.of(new Condition(CODE, partNumber, EQUALS, AND)), DB_NAME)
						.stream()
						.findFirst()
						.map(result -> {
							logger.info("Fetched current part number: {}", result.get(CURRENT_PART_NUMBER));
							return (String) result.get(CURRENT_PART_NUMBER);
						})
						.orElseGet(() -> {
							logger.warn("No current part number found for partNumber: {}", partNumber);
							return "";
						})
		);
	}
	private boolean isInTop2000(String currentPartNumber) {
		logger.info("Checking if partNumber: {} is in the top 2000 based on resulting gross margin.", currentPartNumber);
		return executeSafely(() ->
				genServ.read(INBOUND_STOCK_NRP_MSBI_SALES_VOLUME, List.of(CURRENT_PART_NUMBER, RESULTING_GROSS_MARGIN), new ArrayList<>(), DB_NAME)
						.stream()
						.sorted((r1, r2) -> {
							logger.info("Comparing gross margin: {} vs {}", r1.get(RESULTING_GROSS_MARGIN), r2.get(RESULTING_GROSS_MARGIN));
							return ((BigDecimal) r2.get(RESULTING_GROSS_MARGIN)).compareTo((BigDecimal) r1.get(RESULTING_GROSS_MARGIN));
						})
						.limit(2000)
						.anyMatch(record -> {
							logger.info("Checking if partNumber: {} matches with current part number: {}", currentPartNumber, record.get(CURRENT_PART_NUMBER));
							return currentPartNumber.equals(record.get(CURRENT_PART_NUMBER));
						})
		);
	}
	private String getEccStockingCertCombination(String partNumber) {
		logger.info("Fetching ECC and Stocking Cert for partNumber: {}", partNumber);
		return genServ.read(PART, List.of(ECC, SUGGESTED_STOCKING_CERT), List.of(new Condition(CODE, partNumber, EQUALS, AND)), DB_NAME)
				.stream()
				.findFirst()
				.map(record -> {
					String combination = record.get(ECC) + "_" + record.get(STOCKING_CERT);
					logger.info("Found combination: {}", combination);
					return combination;
				})
				.orElseGet(() -> {
					logger.warn("No part found for partNumber: {}", partNumber);
					return "";
				});
	}
	private boolean isInTop100(String combination) {
		// Log the combination being checked
		logger.info("Checking if combination {} is in the top 100.", combination);

		// Query setup and execution with logging
		return executeSafely(() -> {
			List<Map<String, Object>> salesResult = genServ.read(
					INBOUND_STOCK_NRP_MSBI_SALES_VOLUME,  // Table name
					List.of(ECC, STOCKING_CERT, RESULTING_GROSS_MARGIN),  // Columns to fetch
					new ArrayList<>(),  // No conditions applied (fetching all rows)
					DB_NAME  // Database name
			);

			// Log the query execution and the fetched results
			logger.info("Executed query on stock_nrp_msbi_sales_volume table. Fetched {} rows.", salesResult.size());
			logger.info("Query result: {}", salesResult);

			// Perform the sorting and filtering
			return salesResult.stream()
					.sorted((r1, r2) -> ((BigDecimal) r2.get(RESULTING_GROSS_MARGIN)).compareTo((BigDecimal) r1.get("resulting_gross_margin")))
					.limit(100)
					.anyMatch(record -> (record.get(ECC) + "_" + record.get(STOCKING_CERT)).equals(combination));
		});
	}
	private int calculateExpectationScore(String partNumber) {
		logger.info("Starting to calculate expectation score for partNumber: {}", partNumber);
		return executeSafely(() ->
				fetchStockingCert(partNumber)
						.map(this::fetchExpectationScore)
						.orElse(4)
		);
	}

	private Optional<String> fetchStockingCert(String partNumber) {
		logger.info("Fetching stocking cert for partNumber: {}", partNumber);

		// Log the query being executed
		logger.info("Executing query to fetch stocking cert for partNumber: {}", partNumber);
		return genServ.read(PART, List.of(SUGGESTED_STOCKING_CERT), List.of(new Condition(CODE, partNumber, EQUALS, AND)), DB_NAME)
				.stream()
				.findFirst()
				.map(record -> {
					String stockingCert = (String) record.get(SUGGESTED_STOCKING_CERT);
					logger.info("Fetched stocking cert: {}", stockingCert);
					return stockingCert;
				});
	}

	private int fetchExpectationScore(String stockingCert) {
		logger.info("Fetching expectation score for stockingCert: {}", stockingCert);

		// Log the query being executed
		logger.info("Executing query to fetch expectation score for stockingCert: {}", stockingCert);
		return genServ.read(P_STOCK_CES_MIN_SEGMENT, List.of(CUSTOMER_EXPECTATION_SCORE),
						List.of(new Condition(STOCKING_CERT, stockingCert, EQUALS, AND)), DB_NAME)
				.stream()
				.findFirst()
				.map(record -> {
					int score = (int) record.getOrDefault(CUSTOMER_EXPECTATION_SCORE, 4);
					logger.info("Fetched expectation score: {}", score);
					return score;
				})
				.orElse(4);
	}
	private int calculateCompetitivenessScore(String partNumber) {
		logger.info("Starting to calculate competitiveness score for partNumber: {}", partNumber);
		return executeSafely(() ->
				fetchPartDetailsof(partNumber)
						.filter(part -> hasValidFields(part, ECC, SUGGESTED_STOCKING_CERT))
						.map(part -> {
							String ecc = toString(part.get(ECC));
							String stockingCert = toString(part.get(SUGGESTED_STOCKING_CERT));
							logger.info("Fetched part details: ECC = {}, Stocking Cert = {}", ecc, stockingCert);
							return fetchScore(ecc, stockingCert);
						})
						.orElse(2)
		);
	}

	private Optional<Map<String, Object>> fetchPartDetailsof(String partNumber) {
		logger.info("Fetching part details for partNumber: {}", partNumber);
		// Log the query parameters
		logger.info("Executing query to fetch part details for partNumber: {}", partNumber);
		return genServ.read(PART, List.of(ECC, SUGGESTED_STOCKING_CERT),
						List.of(new Condition(CODE, partNumber, EQUALS, AND)), DB_NAME)
				.stream()
				.findFirst();
	}

	private boolean hasValidFields(Map<String, Object> part, String... fields) {
		logger.info("Validating fields: {}", (Object) fields);
		return List.of(fields).stream().allMatch(field -> part.get(field) != null);
	}

	private int fetchScore(String ecc, String stockingCert) {
		logger.info("Fetching competitiveness score for ECC: {} and StockingCert: {}", ecc, stockingCert);
		// Log the query execution
		logger.info("Executing query to fetch competitiveness score for ECC: {} and StockingCert: {}", ecc, stockingCert);
		return genServ.read(P_STOCK_COMPETITIVENESS_SCORE, List.of(SCORE),
						List.of(new Condition(CERTIFICATION, stockingCert, EQUALS, AND),
								new Condition(ECC, ecc, EQUALS, AND)), DB_NAME)
				.stream()
				.findFirst()
				.map(record -> {
					int score = parseScore(record.get(SCORE));
					logger.info("Fetched competitiveness score: {}", score);
					return score;
				})
				.orElse(2);
	}

	private String toString(Object obj) {
		return obj != null ? obj.toString() : null;
	}

	private int parseScore(Object score) {
		if (score instanceof Long) {
			return ((Long) score).intValue();
		}
		if (score instanceof Integer) {
			return (Integer) score;
		}
		logger.warn("Unexpected score type: {}. Returning default score of 2.", score.getClass().getSimpleName());
		return 2;
	}
}