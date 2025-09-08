package vn.io.nghlong3004.consumer.service.impl;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.client.RestTemplate;
import vn.io.nghlong3004.consumer.model.ACK;
import vn.io.nghlong3004.consumer.model.Message;
import vn.io.nghlong3004.consumer.model.Status;
import vn.io.nghlong3004.consumer.repository.ConsumerRepository;
import vn.io.nghlong3004.consumer.service.ConsumerService;

@Slf4j
@Builder
public class ConsumerServiceImpl implements ConsumerService {

	private final String name;
	private final RestTemplate rest;
	private final ConsumerRepository consumerRepository;
	private final long timeoutMs;
	private final String baseUrl;
	private boolean running;

	@Override
	public void run() {
		while (running) {
			try {
				String url = String.format(baseUrl + "/poll?consumerName=%s&timeoutMs=%d", name, timeoutMs);
				Message item = rest.getForObject(url, Message.class);
				log.info("[{}] read", name);
				url = baseUrl + "/ack";
				ACK ack = new ACK(name, Status.ACK);
				rest.put(url, ack);
			} catch (Exception e) {
				//log.warn("[{}] read error: {}", name, e.toString());
			}
		}
	}
}
