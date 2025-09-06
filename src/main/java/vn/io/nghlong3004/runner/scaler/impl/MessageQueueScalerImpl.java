package vn.io.nghlong3004.runner.scaler.impl;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Builder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vn.io.nghlong3004.runner.queue.MessageQueue;
import vn.io.nghlong3004.runner.scaler.Scaler;

@Builder
public class MessageQueueScalerImpl<T> implements Scaler<T> {

	private static final Logger log = LogManager.getLogger(MessageQueueScalerImpl.class);

	private final MessageQueue<T> messageQueue;
	private final ScheduledExecutorService scheduledExecutorService;

	private final int minCapacity;
	private final int maxCapacity;
	private final double growFactor;
	private final double shrinkFactor;
	private final long windowMs;
	private final long periodMs;
	private final int peakThreshold;
	private final int troughThreshold;
	private final long cooldownMs;
	
	private final AtomicBoolean running = new AtomicBoolean(false);

	private final Deque<Long> peakTimes = new ArrayDeque<>();
	private final Deque<Long> troughTimes = new ArrayDeque<>();
	private long lastScaleAt = 0;

	@Override
	public void start() {
		if (running.compareAndSet(false, true)) {
			log.info("Starting MessageQueueScaler with period={} ms", periodMs);
			scheduledExecutorService.scheduleAtFixedRate(this::tick, periodMs, periodMs,
			                                             TimeUnit.MILLISECONDS);
		} else {
			log.warn("MessageQueueScaler already started");
		}
	}

	@Override
	public void close() {
		if (!running.compareAndSet(true, false)) {
			return;
		}
		try {
			log.info("Shutting down MessageQueueScaler...");
			scheduledExecutorService.shutdownNow();
		} catch (Exception e) {
			log.debug(e.getMessage());
			Thread.currentThread()
			      .interrupt();
		}
	}

	private void tick() {
		int capacity = messageQueue.getCapacity();
		int size = messageQueue.size();
		long now = System.currentTimeMillis();

		peak(now, size, capacity);
		trough(now, size, capacity);
	}

	private void trough(long now, int size, int capacity) {
		if (size == 0) {
			troughTimes.addLast(now);
			evictOld(troughTimes, now - windowMs);
			if (troughTimes.size() >= troughThreshold && (now - lastScaleAt) >= cooldownMs) {
				int newCap = Math.max(minCapacity, (int) Math.floor(capacity / shrinkFactor));
				if (newCap < capacity) {
					messageQueue.setCapacity(newCap);
					lastScaleAt = now;
					troughTimes.clear();
					log.info("Scale DOWN: {} -> {}", capacity, newCap);
				}
			}
		}
	}


	private void peak(long now, int size, int capacity) {
		if (size >= capacity) {
			peakTimes.addLast(now);
			evictOld(peakTimes, now - windowMs);
			if (peakTimes.size() >= peakThreshold && (now - lastScaleAt) >= cooldownMs) {
				int newCap = Math.min(maxCapacity, (int) Math.ceil(capacity * growFactor));
				if (newCap > capacity) {
					messageQueue.setCapacity(newCap);
					lastScaleAt = now;
					peakTimes.clear();
					log.info("Scale UP: {} -> {}", capacity, newCap);
				}
			}
		}
	}

	private void evictOld(Deque<Long> dq, long threshold) {
		while (!dq.isEmpty() && dq.peekFirst() < threshold) {
			dq.removeFirst();
		}
	}
}
