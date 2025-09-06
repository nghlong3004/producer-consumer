package vn.io.nghlong3004.message_queue.service.impl;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import vn.io.nghlong3004.message_queue.exception.ResourceException;
import vn.io.nghlong3004.message_queue.service.MessageQueueScalerService;
import vn.io.nghlong3004.message_queue.service.MessageQueueService;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageQueueScalerImpl implements MessageQueueScalerService {

	private ScheduledExecutorService scheduledExecutorService;
	private final MessageQueueService messageQueue;

	@Value("${message-queue.scaler.capacity.min}")
	private int minCapacity;
	@Value("${message-queue.scaler.capacity.max}")
	private int maxCapacity;
	@Value("${message-queue.scaler.grow.factor}")
	private double growFactor;
	@Value("${message-queue.scaler.shrink.factor}")
	private double shrinkFactor;
	@Value("${message-queue.scaler.window-ms}")
	private long windowMs;
	@Value("${message-queue.scaler.period-ms}")
	private long periodMs;
	@Value("${message-queue.scaler.peak.threshold}")
	private int peakThreshold;
	@Value("${message-queue.scaler.trough.threshold}")
	private int troughThreshold;
	@Value("${message-queue.scaler.cooldown-ms}")
	private long cooldownMs;

	private final AtomicBoolean running = new AtomicBoolean(false);

	private final Deque<Long> peakTimes = new ArrayDeque<>();
	private final Deque<Long> troughTimes = new ArrayDeque<>();
	private long lastScaleAt = 0;

	@Override
	public void start() {
		if (running.compareAndSet(false, true)) {
			log.info("Starting MessageQueueScaler with period={} ms", periodMs);
			if (scheduledExecutorService == null || scheduledExecutorService.isShutdown() ||
			    scheduledExecutorService.isTerminated()) {
				scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> {
					Thread t = new Thread(r, "message-queue-scaler");
					t.setDaemon(true);
					return t;
				});
			}
			peakTimes.clear();
			troughTimes.clear();
			lastScaleAt = 0;
			scheduledExecutorService.scheduleAtFixedRate(this::tick, periodMs, periodMs,
					TimeUnit.MILLISECONDS);
		} else {
			log.warn("MessageQueueScaler already started");
			throw new ResourceException(HttpStatus.BAD_REQUEST, "Already started");
		}
	}

	@Override
	public void close() {
		if (!running.compareAndSet(true, false)) {
			log.warn("MessageQueueScaler already stop");
			throw new ResourceException(HttpStatus.BAD_REQUEST, "Already stop");
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
		int size = messageQueue.getSize();
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
