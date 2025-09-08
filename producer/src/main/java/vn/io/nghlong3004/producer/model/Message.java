package vn.io.nghlong3004.producer.model;

import java.sql.Timestamp;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Message {

	private String senderName;
	private String content;
	private Timestamp created;

}
