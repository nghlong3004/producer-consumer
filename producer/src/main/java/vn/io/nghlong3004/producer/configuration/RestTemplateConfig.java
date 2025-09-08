package vn.io.nghlong3004.producer.configuration;

import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

@Configuration
public class RestTemplateConfig {

	@Bean
	public RestTemplate restTemplate(RestTemplateBuilder restTemplateBuilder,
			@Value("${producer.connect-timeout-ms}") long connectTimeoutMs,
			@Value("${producer.write-timeout-ms}") long writeTimeoutMs) {
		return restTemplateBuilder.connectTimeout(Duration.ofMillis(connectTimeoutMs))
		                          .readTimeout(Duration.ofMillis(writeTimeoutMs))
		                          .build();
	}
}
