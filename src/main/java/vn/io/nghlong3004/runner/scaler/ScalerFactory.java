package vn.io.nghlong3004.runner.scaler;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import vn.io.nghlong3004.configuration.ApplicationContext;
import vn.io.nghlong3004.runner.consumer.ConsumerHandler;
import vn.io.nghlong3004.runner.producer.MessageGenerator;
import vn.io.nghlong3004.runner.queue.MessageQueue;
import vn.io.nghlong3004.runner.scaler.impl.ConsumerScalerImpl;
import vn.io.nghlong3004.runner.scaler.impl.MessageQueueScalerImpl;
import vn.io.nghlong3004.runner.scaler.impl.ProducerScalerImpl;
import vn.io.nghlong3004.util.PropertyUtil;

public final class ScalerFactory {

	private ScalerFactory() {
	}

	public static <T> MessageQueueScalerImpl<T> getMessageQueueScaler(MessageQueue<T> queue) {
		PropertyUtil propertyUtil = ApplicationContext.getPropertyUtil();

		ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
				r -> {
					Thread thread = new Thread(r, "message-queue-auto-scaler");
					thread.setDaemon(true);
					return thread;
				});

		return MessageQueueScalerImpl.<T>builder()
		                             .messageQueue(queue)
		                             .scheduledExecutorService(scheduledExecutorService)
		                             .minCapacity(propertyUtil.getQueueCapacityMin())
		                             .maxCapacity(propertyUtil.getQueueCapacityMax())
		                             .growFactor(propertyUtil.getScalerGrowFactor())
		                             .shrinkFactor(propertyUtil.getScalerShrinkFactor())
		                             .windowMs(propertyUtil.getScalerWindowMs())
		                             .periodMs(propertyUtil.getScalerPeriodMs())
		                             .peakThreshold(propertyUtil.getScalerPeakThreshold())
		                             .troughThreshold(propertyUtil.getScalerTroughThreshold())
		                             .cooldownMs(propertyUtil.getScalerCooldownMs())
		                             .build();
	}

	public static <T> ConsumerScalerImpl<T> getConsumerScaler(MessageQueue<T> queue,
			ConsumerHandler handler) {
		PropertyUtil propertyUtil = ApplicationContext.getPropertyUtil();

		int poolSize = propertyUtil.getConsumerScalerMax();
		ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(poolSize,
				r -> {
					Thread thread = new Thread(r, "consumer-auto-scaler");
					thread.setDaemon(true);
					return thread;
				});

		return ConsumerScalerImpl.<T>builder()
		                         .messageQueue(queue)
		                         .consumerHandler(handler)
		                         .scheduler(scheduledExecutorService)
		                         .paceNanos(propertyUtil.getConsumerPaceNanos())
		                         .initialConsumers(propertyUtil.getConsumerScalerInitial())
		                         .minConsumers(propertyUtil.getConsumerScalerMin())
		                         .maxConsumers(propertyUtil.getConsumerScalerMax())
		                         .stepUp(propertyUtil.getConsumerScalerStepUp())
		                         .stepDown(propertyUtil.getConsumerScalerStepDown())
		                         .samplePeriodMs(propertyUtil.getConsumerScalerSamplePeriodMs())
		                         .holdUpMs(propertyUtil.getConsumerScalerHoldUpMs())
		                         .holdDownMs(propertyUtil.getConsumerScalerHoldDownMs())
		                         .cooldownMs(propertyUtil.getConsumerScalerCooldownMs())
		                         .alpha(propertyUtil.getConsumerScalerAlpha())
		                         .upThreshold(propertyUtil.getConsumerScalerUpThreshold())
		                         .downThreshold(propertyUtil.getConsumerScalerDownThreshold())
		                         .build();
	}


	public static <T> ProducerScalerImpl<T> getProducerScaler(MessageQueue<T> queue,
			MessageGenerator<T> generator) {
		PropertyUtil propertyUtil = ApplicationContext.getPropertyUtil();

		int poolSize = propertyUtil.getProducerScalerMax();
		ScheduledExecutorService ses = Executors.newScheduledThreadPool(poolSize, r -> {
			Thread thread = new Thread(r, "producer");
			thread.setDaemon(true);
			return thread;
		});

		return ProducerScalerImpl.<T>builder()
		                         .messageQueue(queue)
		                         .messageGenerator(generator)
		                         .scheduler(ses)
		                         .paceNanos(propertyUtil.getProducerPaceNanos())
		                         .initialProducers(propertyUtil.getProducerScalerInitial())
		                         .minProducers(propertyUtil.getProducerScalerMin())
		                         .maxProducers(propertyUtil.getProducerScalerMax())
		                         .stepUp(propertyUtil.getProducerScalerStepUp())
		                         .stepDown(propertyUtil.getProducerScalerStepDown())
		                         .samplePeriodMs(propertyUtil.getProducerScalerSamplePeriodMs())
		                         .holdUpMs(propertyUtil.getProducerScalerHoldUpMs())
		                         .holdDownMs(propertyUtil.getProducerScalerHoldDownMs())
		                         .cooldownMs(propertyUtil.getProducerScalerCooldownMs())
		                         .alpha(propertyUtil.getProducerScalerAlpha())
		                         .upThreshold(propertyUtil.getProducerScalerUpThreshold())
		                         .downThreshold(propertyUtil.getProducerScalerDownThreshold())
		                         .build();
	}

}
