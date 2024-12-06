package com.spriced.Stocking_Certification_Level_update.Service.Implementation;

import com.spriced.Stocking_Certification_Level_update.Exceptions.*;
import com.spriced.workflow.DataReading;
import flink.generic.db.Model.Condition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import java.math.BigDecimal;
import java.util.*;

@Service
public class StockingSegmentHHPServiceImpl {
	private static final Logger logger = LoggerFactory.getLogger(StockingSegmentHHPServiceImpl.class);
	private final DataReading genServ;
	public StockingSegmentHHPServiceImpl(DataReading genServ) {
		this.genServ = genServ;
	}
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

	@Value("${ENGINE_MODEL}")
	private String ENGINE_MODEL;

	@Value("${DISPLACEMENT}")
	private String DISPLACEMENT;

	@Value("${CERTIFICATION}")
	private String CERTIFICATION;

	@Value("${RESULTING_GROSS_MARGIN}")
	private String RESULTING_GROSS_MARGIN;

	@Value("${MARKET_APPLICATION}")
	private String MARKET_APPLICATION;

	@Value("${APPLICATION_PRIORITY}")
	private String APPLICATION_PRIORITY;

	@Value("${SCORE}")
	private String SCORE;

	public String update(String partNumber)
			throws Exception {
		logger.info("Starting the process to find the stocking segment for partNumber: {}", partNumber);

		// Fetching part details
		logger.info("Fetching part details for partNumber: {}", partNumber);
		Map<String, Object> part = validateAndFetchPart(partNumber);
		if (part == null || part.isEmpty()) {
			throw new PartNotFoundException("Part not found for partNumber: " + partNumber);
		}
		logger.debug("Fetched part details: {}", part);

		// Validating Product BU
		logger.info("Validating Product BU for partNumber: {}", partNumber);
		String productBU = (String) part.get(PRODUCT_BU);
		validateProductBU(productBU);
		if (productBU == null || productBU.isBlank()) {
			throw new InvalidProductBUException("Invalid Product BU for partNumber: " + partNumber);
		}
		logger.debug("Product BU validated for partNumber: {}", partNumber);

		// Calculating Quality Score
		logger.info("Calculating Quality Score for partNumber: {}", partNumber);
		int qualityScore = calculateQualityScore(partNumber);
		logger.debug("Calculated Quality Score: {}", qualityScore);

		// Fetching Quality Score Segment
		logger.info("Fetching Quality Score Segment for Quality Score: {}", qualityScore);
		String qualityScoreSegment = getQualityScoreSegment(qualityScore);
		if (qualityScoreSegment == null || qualityScoreSegment.isBlank()) {
			throw new QualityScoreNotFoundException("Quality Score Segment not found for Quality Score: " + qualityScore);
		}
		logger.debug("Fetched Quality Score Segment: {}", qualityScoreSegment);

		// Determining final stocking segment
		logger.info("Determining final stocking segment for partNumber: {}", partNumber);
		String finalSegment = determineFinalSegment(partNumber, qualityScoreSegment);
		logger.info("Final stocking segment for partNumber {}: {}", partNumber, finalSegment);

		return finalSegment;
	}



	private String determineFinalSegment(String partNumber, String qualityScoreSegment) throws Exception {
		logger.info("Starting to determine the final segment for partNumber: {}", partNumber);

		logger.info("Fetching minimum segment from certification for partNumber: {}", partNumber);
		String minSegmentFromPstockMinPsbu = fetchHhpMinStockingSegment(partNumber);
		logger.debug("Fetched minimum segment from certification: {}", minSegmentFromPstockMinPsbu);
		logger.info("Comparing all segments to determine the final stocking segment.");
		String finalSegment = compareAndUpdateHhpStockingSegment(
				qualityScoreSegment,minSegmentFromPstockMinPsbu );
		logger.info("Final stocking segment for partNumber {}: {}", partNumber, finalSegment);

		return finalSegment;
	}




	private Map<String, Object> validateAndFetchPart(String partNumber) throws PartNotFoundException {
		logger.info("Starting validation and fetching part details for partNumber: {}", partNumber);

		// Fetch the part details
		logger.debug("Fetching part details for partNumber: {}", partNumber);
		List<Map<String, Object>> partResult = fetchPartDetails(partNumber);

		// Check if the result is empty
		if (partResult.isEmpty()) {
			logger.warn("Part details not found for partNumber: {}", partNumber);
			throw new PartNotFoundException("Part not found for partNumber: " + partNumber);
		}

		// Fetch the first result and log
		Map<String, Object> part = partResult.get(0);
		logger.debug("Fetched part details for partNumber: {}: {}", partNumber, part);

		return part;
	}



	private List<Map<String, Object>> fetchPartDetails(String partNumber) throws PartDetailsFetchException {
		logger.info("Starting to fetch part details for partNumber: {}", partNumber);

		logger.debug("Executing query to fetch details from '{}' table with columns [{}] and condition [code = {}]",
				PART,
				List.of(PRODUCT_BU, CODE, SUGGESTED_STOCKING_CERT, ECC, CURRENT_PART_NUMBER,ENGINE_MODEL, DISPLACEMENT, CERTIFICATION),
				partNumber);

		List<Map<String, Object>> result = genServ.read(
				PART,
				List.of(PRODUCT_BU, CODE, SUGGESTED_STOCKING_CERT, ECC, CURRENT_PART_NUMBER, ENGINE_MODEL, DISPLACEMENT, CERTIFICATION, MARKET_APPLICATION ),
				List.of(new Condition(CODE, partNumber, "=", "AND")),
				DB_NAME
		);

		logger.debug("Query executed successfully. Retrieved {} record(s) for partNumber: {}", result.size(), partNumber);
		return result;
	}



	// Step 2: Validate Product BU
	private void validateProductBU(String productBU) throws InvalidProductBUException {
		logger.info("Validating Product BU: {}", productBU);

		productBU = trimProductBU(productBU);
		logger.debug("Trimmed Product BU: {}", productBU);

		if (!"HHP".equalsIgnoreCase(productBU)) {
			logger.warn("Validation failed. Product BU is not 'HHP'. Found: {}", productBU);
			throw new InvalidProductBUException("Invalid Product BU. Expected 'HHP'. Found: " + productBU);
		}

		logger.info("Product BU validated successfully as 'HHP'.");
	}


	private String trimProductBU(String productBU) {
		logger.debug("Trimming Product BU: {}", productBU);
		String trimmedProductBU = productBU != null ? productBU.trim() : "";
		logger.debug("Trimmed Product BU: {}", trimmedProductBU);
		return trimmedProductBU;
	}

	private int calculateQualityScore(String partNumber) throws ProfitScoreCalculationException, CurrentPartNumberNotFoundException {
		logger.info("Starting Quality Score calculation for partNumber: {}", partNumber);

		// Calculate Profit Score
		logger.info("Calculating Profit Score for partNumber: {}", partNumber);
		int profitScore = calculateProfitScore(partNumber);
		logger.debug("Calculated Profit Score: {}", profitScore);

		// Calculate Competitiveness Score
		logger.info("Calculating Competitiveness Score for partNumber: {}", partNumber);
		int marketApplicationPrice = calculateMarketApplicationPrice(partNumber);
		logger.debug("Calculated Competitiveness Score: {}", marketApplicationPrice);

		// Final Quality Score
		int qualityScore = (2 * profitScore) + marketApplicationPrice;
		logger.info("Final Quality Score for partNumber {}: {}", partNumber, qualityScore);

		return qualityScore;
	}



	public int calculateProfitScore(String partNumber)
			throws CurrentPartNumberNotFoundException, Top2000CheckException, GrossMarginCalculationException {
		logger.info("Starting method {} in class {} for partNumber: {}",
				new Object() {}.getClass().getEnclosingMethod().getName(),
				this.getClass().getSimpleName(),
				partNumber);

		// Fetch part details, including current part number
		logger.info("Fetching part details for partNumber: {}", partNumber);
		List<Map<String, Object>> partDetails = fetchPartDetails(partNumber);

		// Extract current part number
		String currentPartNumber = (String) partDetails.get(0).get(CURRENT_PART_NUMBER);
		if (currentPartNumber == null || currentPartNumber.isEmpty()) {
			logger.error("Current part number not found for partNumber: {}", partNumber);
			throw new CurrentPartNumberNotFoundException("Current part number not found for partNumber: " + partNumber);
		}

		logger.debug("Fetched current part number: {}", currentPartNumber);

		logger.info("Checking if partNumber {} is in the top 2000", partNumber);
		return isInTop2000(currentPartNumber) ? 3 : checkColumnsAndGrossMargin(currentPartNumber);
	}


	private boolean isInTop2000(String currentPartNumber) throws Top2000CheckException {
		logger.info("Starting method {} in class {} for partNumber: {}",
				new Object() {}.getClass().getEnclosingMethod().getName(),
				this.getClass().getSimpleName(),
				currentPartNumber);

		logger.info("Checking if part number {} is in the top 2000", currentPartNumber);

		return genServ.read("stock_nrp_msbi_sales_volume",
						List.of("currentpartnumber", "resultinggrossmargin"),
						new ArrayList<>(),
						DB_NAME)
				.stream()
				.sorted((r1, r2) -> ((BigDecimal) r2.get("resultinggrossmargin"))
						.compareTo((BigDecimal) r1.get("resultinggrossmargin")))
				.limit(2000)
				.anyMatch(record -> currentPartNumber.equals(record.get("currentpartnumber")));
	}



	private int checkColumnsAndGrossMargin(String currentPartNumber) throws ProfitScoreCalculationException {
		logger.info("Starting method {} in class {} for partNumber: {}",
				new Object() {}.getClass().getEnclosingMethod().getName(),
				this.getClass().getSimpleName(),
				currentPartNumber);

		logger.info("Checking columns and gross margin for part number: {}", currentPartNumber);

		return genServ.read("stock_nrp_msbi_sales_volume",
						List.of( "resultinggrossmargin"),
						List.of(new Condition("currentpartnumber", currentPartNumber, "=", "AND")),
						DB_NAME)
				.stream()
				.filter(record -> isValidForProfitScore(record)) // Validate the record
				.map(record -> calculateProfitBasedOnMargin(record)) // Calculate the profit score based on margin
				.findFirst()
				.orElseThrow(() -> {
					logger.warn("No valid profit score found for part number: {}", currentPartNumber);
					return new ProfitScoreCalculationException("No valid profit score found for part number: " + currentPartNumber);
				});
	}



	private boolean isValidForProfitScore(Map<String, Object> record) throws InvalidProfitScoreRecordException {
		logger.info("Starting method {} in class {} to validate the record for profit score",
				new Object() {}.getClass().getEnclosingMethod().getName(),
				this.getClass().getSimpleName());

		if (record.get(ENGINE_MODEL) == null ||
				record.get(DISPLACEMENT) == null ||
				record.get(CERTIFICATION) == null ||
				record.get(ECC) == null) {

			logger.warn("Invalid record detected for profit score calculation: {}", record);
			throw new InvalidProfitScoreRecordException("Record is missing required fields for profit score calculation: " + record);
		}

		logger.debug("Record is valid for profit score calculation.");
		return true;
	}


	private int calculateProfitBasedOnMargin(Map<String, Object> record) throws GrossMarginCalculationException {
		logger.info("Starting method {} in class {} to calculate profit based on margin",
				new Object() {}.getClass().getEnclosingMethod().getName(),
				this.getClass().getSimpleName());

		BigDecimal grossMargin = (BigDecimal) record.get(RESULTING_GROSS_MARGIN);
		if (grossMargin == null) {
			logger.warn("Gross margin is missing in the record: {}", record);
			throw new GrossMarginCalculationException("Gross margin is missing for record: " + record);
		}

		int profitScore = (grossMargin.compareTo(BigDecimal.valueOf(75000)) > 0) ? 2 : 1;

		logger.debug("Calculated profit score based on margin: {} for gross margin: {}", profitScore, grossMargin);
		return profitScore;
	}




	private int calculateMarketApplicationPrice(String partNumber) throws MarketApplicationException {
		logger.info("Calculating Market Application Price for part number: {}", partNumber);

		Optional<String> suggestedMarketApplication = getSuggestedMarketApplication(partNumber);

		if (suggestedMarketApplication.isPresent()) {
			return getMarketApplicationScore(suggestedMarketApplication.get());
		} else {
			logger.warn("No suggested market application found for part number: {}", partNumber);
			throw new MarketApplicationException("No suggested market application found for part number: " + partNumber);
		}
	}


	private Optional<String> getSuggestedMarketApplication(String partNumber) throws SuggestedMarketApplicationException {
		logger.debug("Fetching suggested market application for part number: {}", partNumber);

		return genServ.read("part", List.of("market_application"),
						List.of(new Condition("code", partNumber, "=", "AND")), DB_NAME)
				.stream()
				.findFirst()
				.map(record -> {
					Object marketApplication = record.get("market_application");
					// Convert to String safely using String.valueOf() to handle Long and String types
					return marketApplication != null ? String.valueOf(marketApplication) : null;
				});
	}
	private int getMarketApplicationScore(String suggestedMarketApplication) throws MarketApplicationScoreException {
		logger.debug("Fetching market application score for: {}", suggestedMarketApplication);

		return genServ.read("x_market_application",
						List.of(APPLICATION_PRIORITY, SCORE),
						List.of(new Condition(APPLICATION_PRIORITY, suggestedMarketApplication, "=", "AND")),
						DB_NAME)
				.stream()
				.findFirst()
				.map(record -> (Integer) record.getOrDefault("score", 0))
				.orElseThrow(() -> {
					logger.warn("No market application score found for priority: {}", suggestedMarketApplication);
					return new MarketApplicationScoreException("No market application score found for priority: " + suggestedMarketApplication);
				});
	}



	public String getQualityScoreSegment(int qualityScore) throws QualityScoreSegmentException {
		logger.info("Fetching Quality Score Segment for Quality Score: {}", qualityScore);

		Optional<String> segment = fetchQualityScoreSegment(qualityScore);
		if (segment.isEmpty()) {
			logger.warn("No matching Quality Score Segment found for Quality Score: {}", qualityScore);
			throw new QualityScoreSegmentException("No matching Quality Score Segment found for Quality Score: " + qualityScore);
		} else {
			logger.debug("Fetched Quality Score Segment: {} for Quality Score: {}", segment.get());
		}

		return segment.orElse("No matching segment found");
	}


	private Optional<String> fetchQualityScoreSegment(int qualityScore) throws QualityScoreSegmentException {
		logger.info("Executing query to fetch Quality Score Segment for Quality Score: {}", qualityScore);

		List<Map<String, Object>> result = genServ.read(
				"p_stock_quality_score",
				List.of("hhp_stocking_segment"),
				List.of(new Condition("quality_score", qualityScore, "=", "AND")),
				DB_NAME
		);

		logger.debug("Query executed. Result size: {}", result.size());
		if (result.isEmpty()) {
			logger.warn("No records found for Quality Score: {}", qualityScore);
			throw new QualityScoreSegmentException("No records found for Quality Score: " + qualityScore);
		} else {
			logger.debug("Fetched record: {}", result.get(0));
		}

		return result.stream()
				.findFirst()
				.map(record -> {
					String segment = (String) record.get("hhp_stocking_segment");
					logger.debug("Extracted hhp Stocking Segment: {}", segment);
					return segment;
				});
	}


	public String fetchHhpMinStockingSegment(String partNumber) throws StockingSegmentNotFoundException {
		logger.info("Fetching HHP Min Stocking Segment for partNumber: {}", partNumber);

		return fetchCertificationLevel(partNumber)
				.flatMap(certificationLevel -> fetchStockingSegment(certificationLevel))
				.orElseThrow(() -> {
					logger.warn("No certification level or stocking segment found for partNumber: {}", partNumber);
					return new StockingSegmentNotFoundException("No certification level or stocking segment found for partNumber: " + partNumber);
				});
	}



	private Optional<String> fetchCertificationLevel(String partNumber) {
		return genServ.read("part", List.of("certification_level"),
						List.of(new Condition(CODE, partNumber, "=", "AND")), DB_NAME)
				.stream()
				.findFirst()
				.map(record -> Objects.toString(record.get("certification_level"), null));
	}

	private Optional<String> fetchStockingSegment(String certificationLevel) {
		return certificationLevel == null || certificationLevel.isEmpty() ? Optional.empty() :
				genServ.read("p_Stock_Min_PSBU_Segment", List.of("stocking_segment"),
								List.of(new Condition("certification", certificationLevel, "=", "AND")), DB_NAME)
						.stream()
						.findFirst()
						.map(record -> (String) record.get("stocking_segment"));
	}


	public String compareAndUpdateHhpStockingSegment(String qualityScoreSegment, String minStockingSegmentFromCertificationHhp) throws InvalidComparisonValuesException {
		logger.info("Comparing HHP Min Stocking Segment: {} with HHP Stocking Segment: {}", minStockingSegmentFromCertificationHhp, qualityScoreSegment);

		return Optional.ofNullable(minStockingSegmentFromCertificationHhp)
				.filter(segment -> qualityScoreSegment != null)
				.map(segment -> getSegmentIndex(segment) < getSegmentIndex(qualityScoreSegment)
						? qualityScoreSegment : segment)
				.orElseThrow(() -> new InvalidComparisonValuesException("Invalid comparison values between minStockingSegmentFromCertificationHhp and qualityScoreSegment"));
	}




	// Helper method to get the index of a segment in the predefined order
	private int getSegmentIndex(String segment) throws InvalidSegmentException {
		logger.info("Retrieving index for segment: {}", segment);
		segment = segment.toUpperCase();
		int index = switch (segment) {
			case "A" -> 3;
			case "B" -> 2;
			case "C" -> 1;
			case "D" -> 0;
			default -> {
				logger.warn("Invalid segment: {}. Throwing exception.", segment);
				throw new InvalidSegmentException("Invalid segment: " + segment);
			}
		};

		logger.debug("Segment: {}, Index: {}", segment, index);
		return index;
	}
}