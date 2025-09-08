package vn.io.nghlong3004.consumer.configuration;

import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

@Configuration

public class RestTemplateConfiguration {

	@Bean
	public RestTemplate restTemplate(RestTemplateBuilder restTemplateBuilder,
			@Value("${consumer.connect-timeout-ms}") long connectTimeoutMs,
			@Value("${consumer.read-timeout-ms}") long writeTimeoutMs) {
		return restTemplateBuilder.connectTimeout(Duration.ofMillis(connectTimeoutMs))
		                          .readTimeout(Duration.ofMillis(writeTimeoutMs))
		                          .build();
	}
}
