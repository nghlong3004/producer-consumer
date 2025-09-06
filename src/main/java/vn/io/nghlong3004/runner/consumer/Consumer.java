package vn.io.nghlong3004.runner.consumer;

import lombok.Builder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vn.io.nghlong3004.runner.queue.MessageQueue;

@Builder
public class Consumer<T> implements Runnable {

	private static final Logger log = LogManager.getLogger(Consumer.class);

	private final MessageQueue<T> messageQueue;
	private final ConsumerHandler consumerHandler;
	private final String name;

	@Override
	public void run() {
		try {
			T item = messageQueue.take();
			log.info("[{}] take message -> size = {}", name, messageQueue.size());
			consumerHandler.accept(item);
		} catch (InterruptedException e) {
			Thread.currentThread()
			      .interrupt();
			log.info("[{}] interrupted", name);
		} catch (Exception e) {
			log.error("[{}] error", name, e);
		}
	}
}
