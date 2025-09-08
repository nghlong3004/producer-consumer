package vn.io.nghlong3004.message_queue.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import vn.io.nghlong3004.message_queue.model.Message;
import vn.io.nghlong3004.message_queue.model.dto.ACKRequest;
import vn.io.nghlong3004.message_queue.model.dto.ConsumerResponse;
import vn.io.nghlong3004.message_queue.model.dto.MessageQueueMetric;
import vn.io.nghlong3004.message_queue.model.dto.ProducerRequest;
import vn.io.nghlong3004.message_queue.service.MessageQueueService;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
@Validated
public class MessageQueueController {

	private final MessageQueueService messageQueueService;

	@PostMapping(value = "/put", consumes = MediaType.APPLICATION_JSON_VALUE)
	@ResponseStatus(code = HttpStatus.CREATED)
	public void put(@Valid @RequestBody ProducerRequest producerRequest) {
		messageQueueService.enqueue(Message.builder()
		                                   .senderName(producerRequest.senderName())
		                                   .content(producerRequest.content())
		                                   .created(producerRequest.created())
		                                   .build());
	}

	@GetMapping("/poll")
	@ResponseStatus(code = HttpStatus.OK)
	public ConsumerResponse poll(@RequestParam String consumerName,
			@RequestParam(defaultValue = "0") long timeoutMs) {
		Message message = messageQueueService.poll(consumerName, timeoutMs);
		return new ConsumerResponse(message.getSenderName(), message.getContent(),
				message.getCreated());
	}

	@PutMapping(value = "/ack", consumes = MediaType.APPLICATION_JSON_VALUE)
	@ResponseStatus(code = HttpStatus.OK)
	public void ack(@Valid @RequestBody ACKRequest ackRequest) {
		messageQueueService.handleAck(ackRequest.consumerName(), ackRequest.status());
	}

	@GetMapping("/metric")
	@ResponseStatus(code = HttpStatus.OK)
	public MessageQueueMetric size() {
		return new MessageQueueMetric(messageQueueService.getCapacity(), messageQueueService.getSize());
	}

}
