package vn.io.nghlong3004.message_queue.model.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import vn.io.nghlong3004.message_queue.model.Status;

public record ACKRequest(@NotBlank String consumerName, @NotNull Status status) {

}
