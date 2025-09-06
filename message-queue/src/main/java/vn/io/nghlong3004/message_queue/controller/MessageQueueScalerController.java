package vn.io.nghlong3004.message_queue.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import vn.io.nghlong3004.message_queue.service.MessageQueueScalerService;

@RestController
@RequestMapping("/api/v1/scaler")
@RequiredArgsConstructor
public class MessageQueueScalerController {

	private final MessageQueueScalerService messageQueueScalerService;

	@GetMapping("/start")
	@ResponseStatus(code = HttpStatus.OK)
	public void start() {
		messageQueueScalerService.start();
	}

	@GetMapping("/stop")
	@ResponseStatus(code = HttpStatus.OK)
	public void stop() {
		messageQueueScalerService.close();
	}
}
