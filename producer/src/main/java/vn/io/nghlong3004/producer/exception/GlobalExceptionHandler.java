package vn.io.nghlong3004.producer.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import vn.io.nghlong3004.producer.model.dto.ErrorResponse;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

	@ExceptionHandler(ResourceException.class)
	public ResponseEntity<ErrorResponse> handleBaseException(ResourceException exception) {
		log.warn("An error occurred: {}", exception.getMessage());

		return handleException(exception.getStatus(), exception.getMessage());
	}

	private ResponseEntity<ErrorResponse> handleException(HttpStatus httpStatus, String message) {
		return new ResponseEntity<>(new ErrorResponse(httpStatus.value(), message), httpStatus);
	}
}
