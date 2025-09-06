package vn.io.nghlong3004.runner.scaler.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.Builder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vn.io.nghlong3004.runner.consumer.Consumer;
import vn.io.nghlong3004.runner.consumer.ConsumerHandler;
import vn.io.nghlong3004.runner.queue.MessageQueue;
import vn.io.nghlong3004.runner.scaler.Scaler;

@Builder
public class ConsumerScalerImpl<T> implements Scaler<T> {

	private static final Logger log = LogManager.getLogger(ConsumerScalerImpl.class);

	private final MessageQueue<T> messageQueue;
	private final ConsumerHandler consumerHandler;
	private final ScheduledExecutorService scheduler;

	private final long paceNanos;

	private final int initialConsumers;
	private final int minConsumers;
	private final int maxConsumers;
	private final int stepUp;
	private final int stepDown;

	private final long samplePeriodMs;
	private final long holdUpMs;
	private final long holdDownMs;
	private final long cooldownMs;
	private final double alpha;
	private final double upThreshold;
	private final double downThreshold;

	private final List<ScheduledFuture<?>> consumerTasks = new ArrayList<>();
	private ScheduledFuture<?> controlTask;

	private double utilEma = 0;
	private boolean firstTick = true;
	private long lastScaleAt = 0L;
	private long upSince = -1L;
	private long downSince = -1L;

	@Override
	public synchronized void start() {
		int startCount = clamp(initialConsumers, minConsumers, maxConsumers);
		log.info("Starting consumers: {} (pace={} ms)", startCount,
		         TimeUnit.NANOSECONDS.toMillis(paceNanos));
		scaleTo(startCount);
		controlTask = scheduler.scheduleAtFixedRate(this::controlTick, samplePeriodMs, samplePeriodMs,
		                                            TimeUnit.MILLISECONDS);
	}

	@Override
	public synchronized void close() {
		log.info("Stopping consumers and controller...");
		if (controlTask != null) {
			controlTask.cancel(true);
		}
		cancelAllConsumers();
		scheduler.shutdownNow();
	}

	private void controlTick() {
		try {
			double util = readUtilization();
			updateEma(util);

			long now = System.currentTimeMillis();

			if (utilEma >= upThreshold) {
				markUp(now);
				if (passedHold(upSince, holdUpMs) && canScale(now)) {
					scale(+stepUp);
					afterScale(now);
				}
			} else if (utilEma <= downThreshold) {
				markDown(now);
				if (passedHold(downSince, holdDownMs) && canScale(now)) {
					scale(-stepDown);
					afterScale(now);
				}
			} else {
				resetHysteresis();
			}
		} catch (Throwable t) {
			log.error("Consumer scaler control error", t);
		}
	}

	private double readUtilization() {
		int cap = messageQueue.getCapacity();
		if (cap <= 0) {
			return 0.0;
		}
		int size = messageQueue.size();
		return (double) size / cap;
	}

	private void updateEma(double util) {
		if (firstTick) {
			utilEma = util;
			firstTick = false;
		} else {
			utilEma = alpha * util + (1 - alpha) * utilEma;
		}
	}

	private void markUp(long now) {
		if (upSince < 0) {
			upSince = now;
		}
		downSince = -1;
	}

	private void markDown(long now) {
		if (downSince < 0) {
			downSince = now;
		}
		upSince = -1;
	}

	private void resetHysteresis() {
		upSince = -1;
		downSince = -1;
	}

	private boolean passedHold(long since, long holdMs) {
		return since > 0 && (System.currentTimeMillis() - since) >= holdMs;
	}

	private boolean canScale(long now) {
		return now - lastScaleAt >= cooldownMs;
	}

	private void afterScale(long now) {
		lastScaleAt = now;
		resetHysteresis();
	}

	private synchronized void scale(int delta) {
		int cur = consumerTasks.size();
		int target = clamp(cur + delta, minConsumers, maxConsumers);
		if (target == cur) {
			return;
		}
		log.info("Scale consumers {} -> {} (utilEma={})", cur, target, String.format("%.2f", utilEma));
		scaleTo(target);
	}

	private synchronized void scaleTo(int target) {
		int cur = consumerTasks.size();
		if (target > cur) {
			addConsumers(target - cur, cur);
		} else if (target < cur) {
			removeConsumers(cur - target);
		}
	}

	private void addConsumers(int n, int startIndex) {
		for (int i = 0; i < n; i++) {
			addOneConsumer(startIndex + i);
		}
	}

	private void removeConsumers(int n) {
		for (int i = 0; i < n; ++i) {
			removeOneConsumer();
		}
	}

	private void addOneConsumer(int index) {
		String name = "Consumer-" + index;
		Runnable r = buildConsumerTask(name);
		ScheduledFuture<?> task = scheduler.scheduleAtFixedRate(r, 0, paceNanos, TimeUnit.NANOSECONDS);
		consumerTasks.add(task);
	}

	private void removeOneConsumer() {
		int last = consumerTasks.size() - 1;
		if (last < 0) {
			return;
		}
		ScheduledFuture<?> scheduledFuture = consumerTasks.remove(last);
		scheduledFuture.cancel(true);
	}

	private void cancelAllConsumers() {
		for (ScheduledFuture<?> scheduledFuture : consumerTasks) {
			scheduledFuture.cancel(true);
		}
		consumerTasks.clear();
	}

	private Runnable buildConsumerTask(String name) {
		return Consumer.<T>builder()
		               .messageQueue(messageQueue)
		               .consumerHandler(consumerHandler)
		               .name(name)
		               .build();
	}

	private int clamp(int v, int min, int max) {
		return Math.max(min, Math.min(max, v));
	}
}
