package vn.io.nghlong3004.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PropertyUtil {

	private static final Logger log = LogManager.getLogger(PropertyUtil.class);
	private final Properties properties;
	private static volatile PropertyUtil instance;

	public static PropertyUtil getInstance() {
		if (instance == null) {
			synchronized (PropertyUtil.class) {
				if (instance == null) {
					instance = new PropertyUtil();
				}
			}
		}
		return instance;
	}

	private PropertyUtil() {
		properties = new Properties();
		String fileName = "/application.properties";
		try (InputStream input = getClass().getResourceAsStream(fileName)) {
			if (input != null) {
				properties.load(input);
				log.info("Loaded {}", fileName);
			} else {
				log.error("Not found: {}", fileName);
			}
		} catch (IOException e) {
			log.error("Failed to load {}: {}", fileName, e.getMessage());
		}
	}

	public String getDatasourceDriver() {
		return getValue("datasource.driver-class-name");
	}

	public String getDatasourceUrl() {
		return getValue("datasource.url");
	}

	public String getDatasourceUsername() {
		return getValue("datasource.username");
	}

	public String getDatasourcePassword() {
		return getValue("datasource.password");
	}

	public int getQueueCapacity() {
		return getInt("message-queue.capacity");
	}

	public int getQueueCapacityMin() {
		return getInt("message-queue.capacity.min");
	}

	public int getQueueCapacityMax() {
		return getInt("message-queue.capacity.max");
	}

	public long getScalerWindowMs() {
		return getLong("message-queue.scaler.window-ms");
	}

	public int getScalerPeakThreshold() {
		return getInt("message-queue.scaler.peak-threshold");
	}

	public int getScalerTroughThreshold() {
		return getInt("message-queue.scaler.trough-threshold");
	}

	public long getScalerCooldownMs() {
		return getLong("message-queue.scaler.cooldown-ms");
	}

	public double getScalerGrowFactor() {
		return getDouble("message-queue.scaler.grow.factor");
	}

	public double getScalerShrinkFactor() {
		return getDouble("message-queue.scaler.shrink.factor");
	}

	public long getScalerPeriodMs() {
		return getLong("message-queue.scaler.period-ms");
	}

	public long getConsumerPaceNanos() {
		return getLong("consumer.pace-nanos");
	}

	public long getProducerPaceNanos() {
		return getLong("producer.pace-nanos");
	}

	public int getConsumerScalerInitial() {
		return getInt("consumer.scaler.initial");
	}

	public int getConsumerScalerMin() {
		return getInt("consumer.scaler.min");
	}

	public int getConsumerScalerMax() {
		return getInt("consumer.scaler.max");
	}

	public int getConsumerScalerStepUp() {
		return getInt("consumer.scaler.step-up");
	}

	public int getConsumerScalerStepDown() {
		return getInt("consumer.scaler.step-down");
	}

	public long getConsumerScalerSamplePeriodMs() {
		return getLong("consumer.scaler.sample-period-ms");
	}

	public long getConsumerScalerHoldUpMs() {
		return getLong("consumer.scaler.hold-up-ms");
	}

	public long getConsumerScalerHoldDownMs() {
		return getLong("consumer.scaler.hold-down-ms");
	}

	public long getConsumerScalerCooldownMs() {
		return getLong("consumer.scaler.cooldown-ms");
	}

	public double getConsumerScalerAlpha() {
		return getDouble("consumer.scaler.alpha");
	}

	public double getConsumerScalerUpThreshold() {
		return getDouble("consumer.scaler.up-threshold");
	}

	public double getConsumerScalerDownThreshold() {
		return getDouble("consumer.scaler.down-threshold");
	}

	public int getProducerScalerInitial() {
		return getInt("producer.scaler.initial");
	}

	public int getProducerScalerMin() {
		return getInt("producer.scaler.min");
	}

	public int getProducerScalerMax() {
		return getInt("producer.scaler.max");
	}

	public int getProducerScalerStepUp() {
		return getInt("producer.scaler.step-up");
	}

	public int getProducerScalerStepDown() {
		return getInt("producer.scaler.step-down");
	}

	public long getProducerScalerSamplePeriodMs() {
		return getLong("producer.scaler.sample-period-ms");
	}

	public long getProducerScalerHoldUpMs() {
		return getLong("producer.scaler.hold-up-ms");
	}

	public long getProducerScalerHoldDownMs() {
		return getLong("producer.scaler.hold-down-ms");
	}

	public long getProducerScalerCooldownMs() {
		return getLong("producer.scaler.cooldown-ms");
	}

	public double getProducerScalerAlpha() {
		return getDouble("producer.scaler.alpha");
	}

	public double getProducerScalerUpThreshold() {
		return getDouble("producer.scaler.up-threshold");
	}

	public double getProducerScalerDownThreshold() {
		return getDouble("producer.scaler.down-threshold");
	}

	private String getValue(String key) {
		return properties.getProperty(key);
	}

	private int getInt(String key) {
		return Integer.parseInt(getValue(key));
	}

	private long getLong(String key) {
		return Long.parseLong(getValue(key));
	}

	private double getDouble(String key) {
		return Double.parseDouble(getValue(key));
	}
}
