package vn.io.nghlong3004.message_queue.model.dto;

import java.sql.Timestamp;

public record ConsumerResponse(String senderName, String content, Timestamp created) {

}
