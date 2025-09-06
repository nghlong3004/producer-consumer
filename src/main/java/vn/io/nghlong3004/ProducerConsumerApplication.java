package vn.io.nghlong3004;


import vn.io.nghlong3004.model.Message;
import vn.io.nghlong3004.runner.ProducerConsumerFactory;
import vn.io.nghlong3004.runner.ProducerConsumerRunner;


public class ProducerConsumerApplication {

	public static void main(String[] args) throws InterruptedException {

		ProducerConsumerRunner<Message> producerConsumerRunner = ProducerConsumerFactory.getProducerConsumerRunner();

		producerConsumerRunner.start();

		Thread.currentThread()
		      .join();
		
		producerConsumerRunner.stop();
	}
}
