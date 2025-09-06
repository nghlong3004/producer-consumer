package vn.io.nghlong3004.runner.consumer;

import java.sql.SQLException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import vn.io.nghlong3004.configuration.ApplicationContext;
import vn.io.nghlong3004.model.Message;
import vn.io.nghlong3004.util.DatabaseUtil;

public class ConsumerHandler {

	private final Logger log = LogManager.getLogger(ConsumerHandler.class);

	private final DatabaseUtil databaseUtil;
	private static ConsumerHandler instance;

	private ConsumerHandler(DatabaseUtil databaseUtil) {
		this.databaseUtil = databaseUtil;
	}

	public static ConsumerHandler getInstance() {
		if (instance == null) {
			synchronized (ConsumerHandler.class) {
				instance = new ConsumerHandler(ApplicationContext.getDatabaseUtil());
			}
		}
		return instance;
	}


	public void accept(Object item) {
		if (item instanceof Message message) {
			String query = """
					INSERT INTO message(name, message, created)
					VALUES (?, ?, ?)
					""";
			try {
				databaseUtil.execute(query, message.getName(), message.getMessage(), message.getCreated());
			} catch (SQLException | ClassNotFoundException e) {
				log.debug(e.getMessage());
				throw new RuntimeException(e);
			}

		}
	}
}
