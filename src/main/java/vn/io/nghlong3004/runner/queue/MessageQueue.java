package vn.io.nghlong3004.runner.queue;

import java.util.LinkedList;
import java.util.Queue;

public class MessageQueue<T> {

	private final Queue<T> queue;
	private int capacity;

	public MessageQueue(int capacity) {
		this.queue = new LinkedList<T>();
		this.capacity = capacity;
	}

	public synchronized T take() throws InterruptedException {
		while (queue.isEmpty()) {
			wait();
		}

		T item = queue.remove();
		notifyAll();
		return item;
	}

	public synchronized void put(T item) throws InterruptedException {
		while (queue.size() == this.capacity) {
			wait();
		}

		this.queue.add(item);
		notifyAll();
	}

	public synchronized void setCapacity(int capacity) {
		this.capacity = capacity;
		notifyAll();
	}

	public synchronized int getCapacity() {
		return this.capacity;
	}

	public synchronized int size() {
		return this.queue.size();
	}

}
