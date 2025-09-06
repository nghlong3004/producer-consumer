package vn.io.nghlong3004.runner.producer.message;

import java.sql.Timestamp;
import java.util.UUID;
import vn.io.nghlong3004.model.Message;
import vn.io.nghlong3004.runner.producer.MessageGenerator;

public class DefaultMessageGenerator implements MessageGenerator<Message> {

	@Override
	public Message getMessage(Object... content) {
		String name = (String) content[0];
		String message = UUID.randomUUID()
		                     .toString();
		Timestamp created = new Timestamp(System.currentTimeMillis());
		return Message.builder()
		              .name(name)
		              .message(message)
		              .created(created)
		              .build();
	}
}
