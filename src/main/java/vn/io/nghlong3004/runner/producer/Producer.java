package vn.io.nghlong3004.runner.producer;

import lombok.Builder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vn.io.nghlong3004.runner.queue.MessageQueue;

@Builder
public class Producer<T> implements Runnable {

	private static final Logger log = LogManager.getLogger(Producer.class);

	private final String name;
	private final MessageQueue<T> messageQueue;
	private final MessageGenerator<T> messageGenerator;

	@Override
	public void run() {
		try {
			T item = messageGenerator.getMessage(name);
			messageQueue.put(item);
			log.info("[{}] put message -> size = {}", name, messageQueue.size());
		} catch (InterruptedException e) {
			Thread.currentThread()
			      .interrupt();
			log.info("[{}] interrupted", name);
		} catch (Exception e) {
			log.error("[{}] error", name, e);
		}
	}
}
