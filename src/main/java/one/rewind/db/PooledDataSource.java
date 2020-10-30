package one.rewind.db;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import one.rewind.db.exception.DBInitException;
import one.rewind.db.model.Model;
import one.rewind.util.Configs;

import java.beans.PropertyVetoException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * JDBC 连接池管理器
 * @author karajan
 * @date 2015年1月20日 下午6:19:34
 *
 */
public class PooledDataSource {

	public static Map<String, ComboPooledDataSource> CPDSList = new HashMap<>();

	/**
	 * 获取数据库链接
	 * @param dbName
	 * @return
	 * @throws Exception 
	 */
	public static synchronized ComboPooledDataSource getDataSource(String dbName) throws SQLException, DBInitException {
		
		if(CPDSList.containsKey(dbName)){
			
			ComboPooledDataSource ds = CPDSList.get(dbName);
			Model.logger.trace("{} --> max:{}, busy:{}", dbName, ds.getMaxPoolSize(), ds.getNumBusyConnections());
			
			return ds;
			
		} else {
			
			ComboPooledDataSource ds = addDataSource(dbName);
			Model.logger.trace("{} --> max:{}, busy:{}", dbName, ds.getMaxPoolSize(), ds.getNumBusyConnections());
			
			return ds;
		}
	}
	
	/**
	 * 打开一个数据库连接
	 * @param dbName
	 * @return
	 * @throws Exception
	 */
	public static synchronized ComboPooledDataSource addDataSource(String dbName) throws DBInitException {
		
		ComboPooledDataSource cpds = null;
		cpds = new ComboPooledDataSource();
		
		try {

			Config config = Configs.getConfig(PooledDataSource.class).getConfig(dbName);

			String url = config.getString("url");

			if(url.contains("hive")) {
				cpds.setDriverClass("org.apache.hive.jdbc.HiveDriver");
			}
			else if(url.contains("postgresql")) {
				cpds.setDriverClass("org.postgresql.Driver");
			}
			else {
				cpds.setDriverClass("com.mysql.jdbc.Driver");
			}
			
			cpds.setJdbcUrl(url);
			cpds.setUser(config.getString("username"));
			cpds.setPassword(config.getString("password"));
			
			cpds.setInitialPoolSize(config.getInt("initialPoolSize"));   
			cpds.setMinPoolSize(config.getInt("minPoolSize"));                                     
			cpds.setAcquireIncrement(config.getInt("acquireIncrement"));
			cpds.setMaxPoolSize(config.getInt("maxPoolSize"));
			cpds.setMaxStatements(config.getInt("maxStatements"));
			cpds.setMaxStatementsPerConnection(config.getInt("maxStatements"));
			
			cpds.setMaxConnectionAge(3600);
			cpds.setNumHelperThreads(5);
			cpds.setMaxIdleTimeExcessConnections(120);
			cpds.setMaxIdleTime(120);
			cpds.setAcquireRetryAttempts(0);
			cpds.setAcquireRetryDelay(1000);
			cpds.setIdleConnectionTestPeriod(120);
			
		} catch (ConfigException e) {
			Model.logger.error("DB:[{}] config invalided, ", dbName, e);
			throw new DBInitException("Open pooled db connection failed.");
		} catch (PropertyVetoException e) {
			Model.logger.error("Properties Veto, ", e);
			throw new DBInitException("Open pooled db connection failed.");
		} catch (Exception e) {
			Model.logger.error("Unknown Exception, ", e);
			throw new DBInitException("Open pooled db connection failed.");
		}
		
		CPDSList.put(dbName, cpds);
		
		return cpds;
	}
	
	/**
	 * 关闭所有数据库连接池
	 */
	public static synchronized void close(){
		for(String key : CPDSList.keySet()){
			CPDSList.get(key).close();
		}
	}
}
