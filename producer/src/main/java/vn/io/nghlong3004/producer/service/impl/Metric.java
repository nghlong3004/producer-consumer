package vn.io.nghlong3004.producer.service.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import vn.io.nghlong3004.producer.model.dto.MessageQueueMetric;
import vn.io.nghlong3004.producer.service.MetricService;

@Service
@RequiredArgsConstructor
public class Metric implements MetricService {

	private final RestTemplate rest;

	@Value("${base-url}")
	private String baseUrl;

	@Override
	public MessageQueueMetric fetch() {
		try {
			return rest.getForObject(baseUrl + "/metric", MessageQueueMetric.class);
		} catch (Exception e) {
			return null;
		}
	}
}
