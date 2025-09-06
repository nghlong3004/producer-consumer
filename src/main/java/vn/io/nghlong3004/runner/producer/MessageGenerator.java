package vn.io.nghlong3004.runner.producer;

public interface MessageGenerator<T> {

	T getMessage(Object... content);
}
