package vn.io.nghlong3004.message_queue.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import lombok.experimental.UtilityClass;
import vn.io.nghlong3004.message_queue.model.dto.ErrorResponse;

@UtilityClass
public class JsonErrorUtil {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	public static void write(HttpServletResponse resp, int status, String message)
			throws IOException {
		resp.setStatus(status);
		resp.setContentType("application/json;charset=UTF-8");
		var body = new ErrorResponse(status, message);
		resp.getWriter()
		    .write(MAPPER.writeValueAsString(body));
	}
}
