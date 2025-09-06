package vn.io.nghlong3004.message_queue.service;

import vn.io.nghlong3004.message_queue.model.Message;
import vn.io.nghlong3004.message_queue.model.Status;

public interface MessageQueueService extends AutoCloseable {

	void enqueue(Message message);

	Message poll(String consumerName, long timeoutMillis);

	void handleAck(String consumerName, Status status);

	int getSize();

	int getCapacity();

	void setCapacity(int capacity);
}
