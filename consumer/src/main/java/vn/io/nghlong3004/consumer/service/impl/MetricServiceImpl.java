package vn.io.nghlong3004.consumer.service.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import vn.io.nghlong3004.consumer.model.MessageQueueMetric;
import vn.io.nghlong3004.consumer.service.MetricService;

@Service
@RequiredArgsConstructor
public class MetricServiceImpl implements MetricService {

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
