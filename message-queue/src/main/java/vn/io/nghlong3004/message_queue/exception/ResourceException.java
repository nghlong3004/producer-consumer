package vn.io.nghlong3004.message_queue.exception;

import lombok.Getter;
import org.springframework.http.HttpStatus;

@Getter
public class ResourceException extends RuntimeException {

	private final HttpStatus status;
	private final String message;

	public ResourceException(HttpStatus status, String message) {
		super(message);
		this.message = message;
		this.status = status;
	}

}
