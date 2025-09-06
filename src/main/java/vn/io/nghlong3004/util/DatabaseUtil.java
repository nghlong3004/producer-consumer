package vn.io.nghlong3004.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DatabaseUtil {

	private static final Logger log = LogManager.getLogger(DatabaseUtil.class);

	private final String driver;
	private final String url;
	private final String username;
	private final String password;

	private static DatabaseUtil instance;

	private DatabaseUtil(PropertyUtil propertyUtil) {
		this.driver = propertyUtil.getDatasourceDriver();
		this.url = propertyUtil.getDatasourceUrl();
		this.username = propertyUtil.getDatasourceUsername();
		this.password = propertyUtil.getDatasourcePassword();
	}

	public static DatabaseUtil getInstance(PropertyUtil propertyUtil) {
		if (instance == null) {
			synchronized (DatabaseUtil.class) {
				instance = new DatabaseUtil(propertyUtil);
			}
		}
		return instance;
	}

	private Connection getConnection() throws ClassNotFoundException, SQLException {
		Class.forName(driver);
		return DriverManager.getConnection(url, username, password);
	}

	public List<Object> execute(String query, Object... objects)
			throws SQLException, ClassNotFoundException {
		try (Connection connection = getConnection()) {
			try (PreparedStatement prepareStatement = connection.prepareStatement(query)) {
				addObjectsToPreparedStatement(prepareStatement, objects);
				boolean isSelect = prepareStatement.execute();
				if (isSelect) {
					return select(prepareStatement);
				}
			}
		}
		return null;
	}

	private List<Object> select(PreparedStatement prepareStatement) throws SQLException {
		try (ResultSet resultSet = prepareStatement.getResultSet()) {

			List<Object> result = new ArrayList<>();
			return getResult(result, resultSet);
		}
	}

	private List<Object> getResult(List<Object> result, ResultSet resultSet) throws SQLException {
		List<String> columnNames = getColumnNames(resultSet.getMetaData());
		result.add(columnNames);
		while (resultSet.next()) {
			List<Object> row = new ArrayList<Object>();
			for (String columnName : columnNames) {
				row.add(resultSet.getObject(columnName));
			}
			result.add(row);
		}
		return result;
	}

	private List<String> getColumnNames(ResultSetMetaData resultSetMetaData) throws SQLException {
		List<String> columnNames = new ArrayList<>();
		int columnCount = resultSetMetaData.getColumnCount();
		for (int i = 0; i < columnCount; ++i) {
			columnNames.add(resultSetMetaData.getColumnName(i + 1));
		}
		return columnNames;
	}

	private void addObjectsToPreparedStatement(PreparedStatement prepareStatement, Object... objects)
			throws SQLException {
		if (objects != null) {
			for (int i = 0; i < objects.length; ++i) {
				prepareStatement.setObject(i + 1, objects[i]);
			}
		}
	}


}
