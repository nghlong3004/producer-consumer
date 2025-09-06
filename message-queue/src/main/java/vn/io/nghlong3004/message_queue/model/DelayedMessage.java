package vn.io.nghlong3004.message_queue.model;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import lombok.Data;

@Data
public class DelayedMessage implements Delayed {

	private final Message message;
	private final long readyAtNanos;

	public DelayedMessage(Message message, long delayMillis) {
		this.message = message;
		this.readyAtNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delayMillis);
	}

	@Override
	public long getDelay(TimeUnit timeUnit) {
		long remaining = readyAtNanos - System.nanoTime();
		return timeUnit.convert(remaining, TimeUnit.NANOSECONDS);
	}

	@Override
	public int compareTo(Delayed delayed) {
		long diff = getDelay(TimeUnit.NANOSECONDS) - delayed.getDelay(TimeUnit.NANOSECONDS);
		return Long.compare(diff, 0);
	}
}

