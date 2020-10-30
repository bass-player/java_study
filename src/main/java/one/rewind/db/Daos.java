package one.rewind.db;

import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.jdbc.DataSourceConnectionSource;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.support.DatabaseConnection;
import com.j256.ormlite.table.DatabaseTable;
import one.rewind.db.annotation.DBName;
import one.rewind.db.exception.DBInitException;
import one.rewind.db.model.Model;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * OrmLite 数据对象类连接管理器
 * @author karajan@tfelab.org
 * 2016年3月26日 下午4:17:05
 */
public class Daos {

	private static Map<Class<?>, Dao<?, Object>> daoMap = new HashMap<>();

	/**
	 * 获取指定OrmLite 数据对象类的 Dao对象
	 * @param clazz
	 * @return
	 * @throws Exception 
	 * @throws SQLException
	 */
	public static synchronized <T> Dao<T, Object> get(Class<T> clazz) throws DBInitException, SQLException {
		
		if (daoMap.containsKey(clazz)) {
			return (Dao<T, Object>) daoMap.get(clazz);
		}

		String dbName;

		try {
			dbName = clazz.getAnnotation(DBName.class).value();
			String tableName = clazz.getAnnotation(DatabaseTable.class).tableName();
		} catch (Exception e) {
			Model.logger.error("Error get dbName annotation for {}.", clazz.getName(), e);
			throw new DBInitException("Error get dbName annotation for " + clazz.getName() + ".");
		}
		
		ConnectionSource source = new DataSourceConnectionSource(
				PooledDataSource.getDataSource(dbName),
				PooledDataSource.getDataSource(dbName).getJdbcUrl()
		);

		Dao<T, Object> dao = com.j256.ormlite.dao.DaoManager.createDao(source, clazz);
		daoMap.put(clazz, dao);
		return dao;
	}

	/**
	 * 直接执行SQL语句，需要指定数据库
	 *
	 * @param dbName
	 * @param sql
	 * @return
	 * @throws DBInitException
	 * @throws SQLException
	 */
	public static boolean exec(String dbName, String sql) throws DBInitException, SQLException {

		ConnectionSource source = new DataSourceConnectionSource(
				PooledDataSource.getDataSource(dbName),
				PooledDataSource.getDataSource(dbName).getJdbcUrl()
		);

		DatabaseConnection conn = source.getReadWriteConnection();

		boolean result = conn.executeStatement(sql, DatabaseConnection.DEFAULT_RESULT_FLAGS) == DatabaseConnection.DEFAULT_RESULT_FLAGS;
		conn.close();
		return result;
	}
}