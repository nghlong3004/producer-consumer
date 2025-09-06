package vn.io.nghlong3004.model;

import java.sql.Timestamp;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Message {

	private String message;
	private Timestamp created;
	private String name;
	
}
