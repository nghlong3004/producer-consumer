package vn.io.nghlong3004.producer.service.impl;

import jakarta.annotation.Nonnull;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import vn.io.nghlong3004.producer.model.dto.MessageQueueMetric;
import vn.io.nghlong3004.producer.service.MetricService;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProducerScaler implements ApplicationRunner {

	private ScheduledExecutorService producerPool;
	private ScheduledExecutorService controlPool;
	private final RestTemplate rest;
	private final MetricService metrics;

	@Value("${base-url}")
	private String baseUrl;

	@Value("${producer.scaler.pace-ms}")
	private long paceMs;

	@Value("${producer.scaler.initial}")
	private int initialProducers;
	@Value("${producer.scaler.min}")
	private int minProducers;
	@Value("${producer.scaler.max}")
	private int maxProducers;
	@Value("${producer.scaler.step-up}")
	private int stepUp;
	@Value("${producer.scaler.step-down}")
	private int stepDown;
	@Value("${producer.scaler.sample-period-ms}")
	private long samplePeriodMs;
	@Value("${producer.scaler.hold-up-ms}")
	private long holdUpMs;
	@Value("${producer.scaler.hold-down-ms}")
	private long holdDownMs;
	@Value("${producer.scaler.cooldown-ms}")
	private long cooldownMs;
	@Value("${producer.scaler.alpha}")
	private double alpha;
	@Value("${producer.scaler.up-threshold}")
	private double upThreshold;
	@Value("${producer.scaler.down-threshold}")
	private double downThreshold;

	private final List<ScheduledFuture<?>> producerTasks = new ArrayList<>();
	private ScheduledFuture<?> controlTask;

	private double utilEma = 0.0;
	private boolean firstTick = true;
	private long lastScaleAt = 0L;
	private long highSince = -1L;
	private long lowSince = -1L;

	@PostConstruct
	public void init() {
		producerPool = Executors.newScheduledThreadPool(Math.max(1, maxProducers),
				new NamedThreadFactory("producer-"));
		controlPool = Executors.newScheduledThreadPool(1, new NamedThreadFactory("producer-ctrl-"));
	}

	@Override
	public void run(ApplicationArguments args) {
		int startCount = clamp(initialProducers, minProducers, maxProducers);
		log.info("Starting {} producers (pace={} ms)", startCount, paceMs);
		scaleTo(startCount);
		controlTask = controlPool.scheduleAtFixedRate(this::controlTick, samplePeriodMs, samplePeriodMs,
				TimeUnit.MILLISECONDS);
	}

	@PreDestroy
	public void shutdown() {
		log.info("Stopping producers and controller...");
		if (controlTask != null) {
			controlTask.cancel(true);
		}
		cancelAllProducers();
		controlPool.shutdownNow();
		producerPool.shutdownNow();
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
		MessageQueueMetric messageQueueStatus = metrics.fetch();
		log.info("{}", messageQueueStatus);
		if (messageQueueStatus == null) {
			throw new RuntimeException("Metrics is null");
		}
		if (messageQueueStatus.capacity() <= 0) {
			return 0;
		}
		return Math.min(1.0,
				Math.max(0.0, (double) messageQueueStatus.size() / messageQueueStatus.capacity()));
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
		long jitter = Math.max(0, paceMs / 5);
		long initialDelay = (jitter == 0) ? 0 : ThreadLocalRandom.current()
		                                                         .nextLong(jitter);
		ScheduledFuture<?> task = producerPool.scheduleAtFixedRate(runnable, initialDelay, paceMs,
				TimeUnit.MILLISECONDS);
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
		return Producer.builder()
		               .name(name)
		               .rest(rest)
		               .baseUrl(baseUrl)
		               .running(true)
		               .build();
	}

	private int clamp(int v, int min, int max) {
		return Math.max(min, Math.min(max, v));
	}

	private static class NamedThreadFactory implements ThreadFactory {

		private final String prefix;
		private final AtomicInteger seq = new AtomicInteger(1);

		NamedThreadFactory(String prefix) {
			this.prefix = prefix;
		}

		@Override
		public Thread newThread(@Nonnull Runnable r) {
			Thread t = new Thread(r, prefix + seq.getAndIncrement());
			t.setDaemon(true);
			return t;
		}
	}
}
