package vn.io.nghlong3004.configuration;

import lombok.NoArgsConstructor;
import vn.io.nghlong3004.util.DatabaseUtil;
import vn.io.nghlong3004.util.PropertyUtil;

@NoArgsConstructor
public class ApplicationContext {

	private static final PropertyUtil PROPERTY_UTIL = PropertyUtil.getInstance();
	private static final DatabaseUtil DATABASE_UTIL = DatabaseUtil.getInstance(PROPERTY_UTIL);

	public static PropertyUtil getPropertyUtil() {
		return PROPERTY_UTIL;
	}

	public static DatabaseUtil getDatabaseUtil() {
		return DATABASE_UTIL;
	}
}
