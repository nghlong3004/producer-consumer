package vn.io.nghlong3004.runner;

import vn.io.nghlong3004.model.Message;
import vn.io.nghlong3004.runner.producer.MessageGenerator;
import vn.io.nghlong3004.runner.producer.message.DefaultMessageGenerator;

public class ProducerConsumerFactory {

	private ProducerConsumerFactory() {
	}

	public static ProducerConsumerRunner<Message> getProducerConsumerRunner() {
		MessageGenerator<Message> messageGenerator = new DefaultMessageGenerator();

		return new ProducerConsumerRunner<>(messageGenerator);
	}

}
