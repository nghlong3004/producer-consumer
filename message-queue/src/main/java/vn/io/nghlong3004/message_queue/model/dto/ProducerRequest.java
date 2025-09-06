package vn.io.nghlong3004.message_queue.model.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.sql.Timestamp;

public record ProducerRequest(@NotBlank String senderName, @NotBlank String content,
                              @NotNull Timestamp created) {

}
