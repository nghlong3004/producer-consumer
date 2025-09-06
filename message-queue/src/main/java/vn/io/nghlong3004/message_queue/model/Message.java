package vn.io.nghlong3004.message_queue.model;

import java.sql.Timestamp;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class Message {

	private Long id;
	private String senderName;
	private String content;
	private MessageStatus status;
	private Timestamp created;

}
