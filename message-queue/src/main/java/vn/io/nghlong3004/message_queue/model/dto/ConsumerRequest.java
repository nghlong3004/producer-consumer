package vn.io.nghlong3004.message_queue.model.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record ConsumerRequest(@NotBlank String consumerName, @Min(0) long timeoutMs) {

}
