package vn.io.nghlong3004.consumer.model;

import java.sql.Timestamp;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class Message {
	
	private String senderName;
	private String content;
	private Timestamp created;

}
