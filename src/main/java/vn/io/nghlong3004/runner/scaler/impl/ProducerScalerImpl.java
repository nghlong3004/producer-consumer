package vn.io.nghlong3004.runner.scaler.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.Builder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vn.io.nghlong3004.runner.producer.MessageGenerator;
import vn.io.nghlong3004.runner.producer.Producer;
import vn.io.nghlong3004.runner.queue.MessageQueue;
import vn.io.nghlong3004.runner.scaler.Scaler;

@Builder
public class ProducerScalerImpl<T> implements Scaler<T> {

	private static final Logger log = LogManager.getLogger(ProducerScalerImpl.class);

	private final MessageQueue<T> messageQueue;
	private final MessageGenerator<T> messageGenerator;
	private final ScheduledExecutorService scheduler;

	private final long paceNanos;

	private final int initialProducers;
	private final int minProducers;
	private final int maxProducers;
	private final int stepUp;
	private final int stepDown;

	private final long samplePeriodMs;
	private final long holdUpMs;
	private final long holdDownMs;
	private final long cooldownMs;
	private final double alpha;
	private final double upThreshold;
	private final double downThreshold;

	private final List<ScheduledFuture<?>> producerTasks = new ArrayList<>();
	private ScheduledFuture<?> controlTask;

	private double utilEma = 0.0;
	private boolean firstTick = true;
	private long lastScaleAt = 0L;
	private long highSince = -1L;
	private long lowSince = -1L;

	@Override
	public synchronized void start() {
		int startCount = clamp(initialProducers, minProducers, maxProducers);
		log.info("Starting {} producers (pace={} ms)", startCount,
		         TimeUnit.NANOSECONDS.toMillis(paceNanos));
		scaleTo(startCount);
		controlTask = scheduler.scheduleAtFixedRate(this::controlTick, samplePeriodMs, samplePeriodMs,
		                                            TimeUnit.MILLISECONDS);
	}

	@Override
	public synchronized void close() {
		log.info("Stopping producers and controller...");
		if (controlTask != null) {
			controlTask.cancel(true);
		}
		cancelAllProducers();
		scheduler.shutdownNow();
	}

	private void controlTick() {
		try {
			double util = readUtilization();
			updateEwma(util);

			long now = System.currentTimeMillis();

			if (utilEma >= upThreshold) {
				if (highSince < 0) {
					highSince = now;
				}
				lowSince = -1;
				if (passed(highSince, holdUpMs) && canScale(now)) {
					scale(-stepDown);
					afterScale(now);
				}
			} else if (utilEma <= downThreshold) {
				if (lowSince < 0) {
					lowSince = now;
				}
				highSince = -1;
				if (passed(lowSince, holdDownMs) && canScale(now)) {
					scale(+stepUp);
					afterScale(now);
				}
			} else {
				resetHysteresis();
			}
		} catch (Throwable t) {
			log.error("Producer scaler control error", t);
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

	private void updateEwma(double util) {
		if (firstTick) {
			utilEma = util;
			firstTick = false;
		} else {
			utilEma = alpha * util + (1 - alpha) * utilEma;
		}
	}

	private boolean passed(long since, long holdMs) {
		return since > 0 && (System.currentTimeMillis() - since) >= holdMs;
	}

	private boolean canScale(long now) {
		return now - lastScaleAt >= cooldownMs;
	}

	private void afterScale(long now) {
		lastScaleAt = now;
		resetHysteresis();
	}

	private void resetHysteresis() {
		highSince = -1;
		lowSince = -1;
	}

	private synchronized void scale(int delta) {
		int cur = producerTasks.size();
		int target = clamp(cur + delta, minProducers, maxProducers);
		if (target == cur) {
			return;
		}
		log.info("Scale producers {} -> {} (utilEma={})", cur, target, String.format("%.2f", utilEma));
		scaleTo(target);
	}

	private synchronized void scaleTo(int target) {
		int cur = producerTasks.size();
		if (target > cur) {
			addProducers(target - cur, cur);
		} else if (target < cur) {
			removeProducers(cur - target);
		}
	}

	private void addProducers(int n, int startIndex) {
		for (int i = 0; i < n; ++i) {
			addOneProducer(startIndex + i);
		}
	}

	private void removeProducers(int n) {
		for (int i = 0; i < n; ++i) {
			removeOneProducer();
		}
	}

	private void addOneProducer(int index) {
		String name = "Producer-" + index;
		Runnable runnable = buildProducerTask(name);
		ScheduledFuture<?> task = scheduler.scheduleAtFixedRate(runnable, 0, paceNanos,
		                                                        TimeUnit.NANOSECONDS);
		producerTasks.add(task);
	}

	private void removeOneProducer() {
		int last = producerTasks.size() - 1;
		if (last < 0) {
			return;
		}
		ScheduledFuture<?> scheduledFuture = producerTasks.remove(last);
		scheduledFuture.cancel(true);
	}

	private void cancelAllProducers() {
		for (ScheduledFuture<?> scheduledFuture : producerTasks) {
			scheduledFuture.cancel(true);
		}
		producerTasks.clear();
	}

	private Runnable buildProducerTask(String name) {
		return Producer.<T>builder()
		               .name(name)
		               .messageQueue(messageQueue)
		               .messageGenerator(messageGenerator)
		               .build();
	}

	private int clamp(int v, int min, int max) {
		return Math.max(min, Math.min(max, v));
	}
}
