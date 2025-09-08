package vn.io.nghlong3004.producer.service.impl;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.client.RestTemplate;
import vn.io.nghlong3004.producer.model.Message;
import vn.io.nghlong3004.producer.util.MessageGenerator;

@Slf4j
@Builder
public class Producer implements Runnable {

	private final String name;
	private final RestTemplate rest;
	private final String baseUrl;
	private boolean running;

	@Override
	public void run() {
		while (running) {
			try {
				Message item = MessageGenerator.getMessage(name);
				rest.postForObject(baseUrl + "/put", item, Message.class);
				log.info("[{}] sent", name);
			} catch (Exception e) {
				//log.warn("[{}] send error: {}", name, e.toString());
			}
		}
	}
}
