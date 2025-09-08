package vn.io.nghlong3004.consumer.service.impl;

import jakarta.annotation.Nonnull;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import vn.io.nghlong3004.consumer.model.MessageQueueMetric;
import vn.io.nghlong3004.consumer.repository.ConsumerRepository;
import vn.io.nghlong3004.consumer.service.ConsumerScalerService;
import vn.io.nghlong3004.consumer.service.MetricService;

@Slf4j
@Service
@RequiredArgsConstructor
public class ConsumerScalerSerivceImpl implements ConsumerScalerService {

	private final RestTemplate rest;
	private final ConsumerRepository consumerRepository;
	private final MetricService metrics;
	private ScheduledExecutorService consumerPool;
	private ScheduledExecutorService controlPool;

	@Value("${base-url}")
	private String baseUrl;

	@Value("${consumer.scaler.pace-ms}")
	private long paceMs;
	@Value("${consumer.connect-timeout-ms}")
	private long timeoutMs;

	@Value("${consumer.scaler.initial}")
	private int initialConsumers;
	@Value("${consumer.scaler.min}")
	private int minConsumers;
	@Value("${consumer.scaler.max}")
	private int maxConsumers;
	@Value("${consumer.scaler.step-up}")
	private int stepUp;
	@Value("${consumer.scaler.step-down}")
	private int stepDown;
	@Value("${consumer.scaler.sample-period-ms}")
	private long samplePeriodMs;
	@Value("${consumer.scaler.hold-up-ms}")
	private long holdUpMs;
	@Value("${consumer.scaler.hold-down-ms}")
	private long holdDownMs;
	@Value("${consumer.scaler.cooldown-ms}")
	private long cooldownMs;
	@Value("${consumer.scaler.alpha}")
	private double alpha;
	@Value("${consumer.scaler.up-threshold}")
	private double upThreshold;
	@Value("${consumer.scaler.down-threshold}")
	private double downThreshold;

	private final List<ScheduledFuture<?>> consumerTasks = new ArrayList<>();
	private ScheduledFuture<?> controlTask;

	private double utilEma = 0;
	private boolean firstTick = true;
	private long lastScaleAt = 0L;
	private long upSince = -1L;
	private long downSince = -1L;

	@PostConstruct
	public void init() {
		consumerPool = Executors.newScheduledThreadPool(Math.max(1, maxConsumers),
				new NamedThreadFactory("producer-"));
		controlPool = Executors.newScheduledThreadPool(1, new NamedThreadFactory("producer-ctrl-"));
	}

	@Override
	public void run(ApplicationArguments args) {
		int startCount = clamp(initialConsumers, minConsumers, maxConsumers);
		log.info("Starting consumers: {} (pace={} ms)", startCount, paceMs);
		scaleTo(startCount);
		controlTask = controlPool.scheduleAtFixedRate(this::controlTick, samplePeriodMs, samplePeriodMs,
				TimeUnit.MILLISECONDS);
	}

	@PreDestroy
	public void shutdown() {
		log.info("Stopping consumers and controller...");
		if (controlTask != null) {
			controlTask.cancel(true);
		}
		cancelAllConsumers();
		consumerPool.shutdownNow();
		controlPool.shutdownNow();
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
		ScheduledFuture<?> task = consumerPool.scheduleAtFixedRate(r, 0, paceMs, TimeUnit.MILLISECONDS);
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
		return ConsumerServiceImpl.builder()
		                          .consumerRepository(consumerRepository)
		                          .name(name)
		                          .rest(rest)
		                          .baseUrl(baseUrl)
		                          .timeoutMs(timeoutMs)
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
