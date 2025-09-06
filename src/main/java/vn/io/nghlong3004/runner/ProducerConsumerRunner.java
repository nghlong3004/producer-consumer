package vn.io.nghlong3004.runner;

import vn.io.nghlong3004.configuration.ApplicationContext;
import vn.io.nghlong3004.runner.consumer.ConsumerHandler;
import vn.io.nghlong3004.runner.producer.MessageGenerator;
import vn.io.nghlong3004.runner.queue.MessageQueue;
import vn.io.nghlong3004.runner.scaler.Scaler;
import vn.io.nghlong3004.runner.scaler.ScalerFactory;
import vn.io.nghlong3004.util.PropertyUtil;

public class ProducerConsumerRunner<T> {

	private final Scaler<T> messageQueueScaler;
	private final Scaler<T> producerScaler;
	private final Scaler<T> consumerScaler;

	public ProducerConsumerRunner(MessageGenerator<T> messageGenerator) {
		PropertyUtil propertyUtil = ApplicationContext.getPropertyUtil();
		MessageQueue<T> messageQueue = new MessageQueue<>(propertyUtil.getQueueCapacity());
		this.messageQueueScaler = ScalerFactory.getMessageQueueScaler(messageQueue);
		this.producerScaler = ScalerFactory.getProducerScaler(messageQueue, messageGenerator);
		this.consumerScaler = ScalerFactory.getConsumerScaler(messageQueue,
		                                                      ConsumerHandler.getInstance());
	}

	public void start() {
		messageQueueScaler.start();
		producerScaler.start();
		consumerScaler.start();

		Runtime.getRuntime()
		       .addShutdownHook(new Thread(this::stop, "shutdown-hook"));
	}

	public void stop() {
		try {
			producerScaler.close();
		} catch (Exception e) {
			throw new RuntimeException("Error closing producer scaler", e);
		}

		try {
			consumerScaler.close();
		} catch (Exception e) {
			throw new RuntimeException("Error closing consumer scaler", e);
		}

		try {
			messageQueueScaler.close();
		} catch (Exception e) {
			throw new RuntimeException("Error closing message queue scaler", e);
		}
	}
}
