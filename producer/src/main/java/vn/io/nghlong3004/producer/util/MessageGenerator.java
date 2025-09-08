package vn.io.nghlong3004.producer.util;

import java.sql.Timestamp;
import java.util.UUID;
import vn.io.nghlong3004.producer.model.Message;

public final class MessageGenerator {

	private MessageGenerator() {
	}

	public static Message getMessage(String name) {
		String content = UUID.randomUUID()
		                     .toString();
		Timestamp created = new Timestamp(System.currentTimeMillis());
		return new Message(name, content, created);
	}
}
