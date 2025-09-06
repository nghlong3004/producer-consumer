package vn.io.nghlong3004.message_queue.service.impl;

import jakarta.annotation.PostConstruct;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import vn.io.nghlong3004.message_queue.exception.ResourceException;
import vn.io.nghlong3004.message_queue.model.DelayedMessage;
import vn.io.nghlong3004.message_queue.model.Message;
import vn.io.nghlong3004.message_queue.model.MessageStatus;
import vn.io.nghlong3004.message_queue.model.Status;
import vn.io.nghlong3004.message_queue.service.MessageQueueService;

@Slf4j
@Service
public class MessageQueueImpl implements MessageQueueService {

	private final DelayQueue<DelayedMessage> delayQueue = new DelayQueue<>();

	private final PriorityBlockingQueue<Message> readyQueue = new PriorityBlockingQueue<>(1 << 10,
			Comparator.comparing(Message::getCreated));

	private final ConcurrentMap<String, Message> inProgress = new ConcurrentHashMap<>();

	private final AtomicBoolean running = new AtomicBoolean(false);

	private final ReentrantLock lock = new ReentrantLock();
	private final Condition notFull = lock.newCondition();
	private final Condition notEmpty = lock.newCondition();

	private Thread promoterThread;

	@Value("${message-queue.promoter-wait-step-ms}")
	private long promoterWaitStepMs;

	@Value("${message-queue.enqueue-timeout-ms}")
	private long enqueueTimeoutMs;

	@Value("${message-queue.delay-ms}")
	private long delayMillis;

	@Value("${message-queue.capacity}")
	private int capacity;

	@PostConstruct
	void init() {
		if (delayMillis < 0) {
			log.warn("Configured delayMillis < 0 ({}). Forcing to 0.", delayMillis);
			delayMillis = 0;
		}

		running.set(true);
		promoterThread = new Thread(this::promoteLoop, "message-queue-promoter");
		promoterThread.setDaemon(true);
		promoterThread.start();
		log.info("MessageQueue initialized. delayMillis={}ms", delayMillis);
	}

	@Override
	public void enqueue(Message message) {
		message.setStatus(MessageStatus.NOT_READY);

		lock.lock();
		try {
			long nanos = TimeUnit.MILLISECONDS.toNanos(Math.max(0, enqueueTimeoutMs));
			while (getSize() >= getCapacity()) {
				if (nanos <= 0L) {
					log.debug("enqueue -> queue is full (cap={}), timed out after {} ms", getCapacity(),
							enqueueTimeoutMs);
					throw new ResourceException(HttpStatus.BAD_REQUEST, "MessageQueue is full");
				}
				nanos = notFull.awaitNanos(nanos);
			}
		} catch (InterruptedException e) {
			Thread.currentThread()
			      .interrupt();
			log.debug("Interrupted while enqueueing");
			throw new ResourceException(HttpStatus.INTERNAL_SERVER_ERROR, "Internal server error");
		} finally {
			lock.unlock();
		}

		delayQueue.offer(new DelayedMessage(message, delayMillis));
		log.info("enqueue -> id={}, from={}, created={}, status={}. notReadySize={}", message.getId(),
				message.getSenderName(), message.getCreated(), message.getStatus(), delayQueue.size());
	}

	@Override
	public Message poll(String consumerName, long timeoutMillis) {
		if (inProgress.containsKey(consumerName)) {
			throw new IllegalStateException(
					"Consumer " + consumerName + " is already processing a message");
		}

		Message message = null;
		try {
			message =
					(timeoutMillis <= 0) ? readyQueue.poll() : readyQueue.poll(0, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			log.debug("Interrupted but still poll");
			throw new ResourceException(HttpStatus.INTERNAL_SERVER_ERROR, "Internal server error");
		}
		if (message == null && timeoutMillis > 0) {
			lock.lock();
			try {
				long nanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
				while ((message = readyQueue.poll()) == null) {
					if (nanos <= 0L) {
						log.trace("poll -> consumer={} timed out. readySize={}", consumerName,
								readyQueue.size());
						throw new ResourceException(HttpStatus.BAD_REQUEST, "MessageQueue is empty");
					}
					nanos = notEmpty.awaitNanos(nanos);
				}
			} catch (InterruptedException e) {
				log.debug(e.getMessage());
				throw new ResourceException(HttpStatus.INTERNAL_SERVER_ERROR, "Internal server error");
			} finally {
				lock.unlock();
			}
		}

		if (message == null) {
			log.trace("poll -> consumer={} no message available. readySize={}", consumerName,
					readyQueue.size());
			throw new ResourceException(HttpStatus.BAD_REQUEST, "MessageQueue is empty");
		}

		message.setStatus(MessageStatus.IN_PROGRESS);
		inProgress.put(consumerName, message);
		lock.lock();
		try {
			notFull.signal();
		} finally {
			lock.unlock();
		}

		log.debug("poll -> consumer={} got id={}, created={}, readySize={}, inProgressSize={}",
				consumerName, message.getId(), message.getCreated(), readyQueue.size(), inProgress.size());
		return message;
	}

	@Override
	public void handleAck(String consumerName, Status status) {
		switch (status) {
			case ACK -> ack(consumerName);
			case NACK -> nack(consumerName);
			default -> log.warn("Unknown status {} by consumer={}", status, consumerName);
		}
	}

	private void ack(String consumerName) {
		Message message = inProgress.remove(consumerName);
		if (message == null) {
			log.warn("ack -> consumer={} has no in-progress message. Nothing to ack.", consumerName);
			throw new ResourceException(HttpStatus.BAD_REQUEST, "no in-progress message. Nothing to ack");
		}
		log.info("ack -> consumer={} id={} removed. inProgressSize={}", consumerName, message.getId(),
				inProgress.size());
	}

	public void nack(String consumerName) {
		Message message = inProgress.remove(consumerName);
		if (message == null) {
			log.warn("nack -> consumer={} has no in-progress message. Nothing to return.", consumerName);
			throw new ResourceException(HttpStatus.BAD_REQUEST,
					"has no in-progress message. Nothing to nack");
		}
		message.setStatus(MessageStatus.READY);
		readyQueue.offer(message);
		log.info("nack -> consumer={} id={} returned to READY. readySize={}, inProgressSize={}",
				consumerName, message.getId(), readyQueue.size(), inProgress.size());
	}

	@Override
	public int getSize() {
		return this.delayQueue.size() + this.readyQueue.size() + this.inProgress.size();
	}

	@Override
	public int getCapacity() {
		return this.capacity;
	}

	@Override
	public void setCapacity(int capacity) {
		this.capacity = capacity;
	}

	@Override
	public void close() {
		if (!running.compareAndSet(true, false)) {
			return;
		}
		if (promoterThread != null) {
			promoterThread.interrupt();
		}
		log.info("MessageQueue shutting down...");
	}

	private void promoteLoop() {
		log.debug("Promoter thread started. delayMillis={}ms", delayMillis);
		final long stepMs = Math.max(1L, promoterWaitStepMs);
		while (running.get()) {
			try {
				DelayedMessage delayed = delayQueue.take();
				Message message = delayed.getMessage();
				lock.lockInterruptibly();
				try {
					while (running.get() && getSize() >= capacity) {
						notFull.await(stepMs, TimeUnit.MILLISECONDS);
					}
				} finally {
					lock.unlock();
				}
				if (!running.get()) {
					break;
				}

				message.setStatus(MessageStatus.READY);
				readyQueue.offer(message);

				log.info("Promoter -> moved to READY. notReadySize={}, readySize={}", delayQueue.size(),
						readyQueue.size());
			} catch (InterruptedException e) {
				if (!running.get()) {
					break;
				}
				log.debug("Promoter interrupted but still running.");
				throw new ResourceException(HttpStatus.INTERNAL_SERVER_ERROR, "Internal server error");
			} catch (Throwable t) {
				log.error("Promoter loop error: {}", t.getMessage(), t);
				throw new ResourceException(HttpStatus.INTERNAL_SERVER_ERROR, "Internal server error");
			}
		}
		log.debug("Promoter thread stopped.");
	}
}
