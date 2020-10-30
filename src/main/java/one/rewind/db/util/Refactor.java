package one.rewind.db.util;

import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.jdbc.DataSourceConnectionSource;
import com.j256.ormlite.support.ConnectionSource;
import com.j256.ormlite.table.TableUtils;
import one.rewind.db.Daos;
import one.rewind.db.PooledDataSource;
import one.rewind.db.annotation.DBName;
import one.rewind.db.exception.DBInitException;
import one.rewind.db.model.Model;
import org.reflections.Reflections;

import java.lang.annotation.Annotation;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * 基于OrmLite的数据辅助工具
 * 自动建表 删除表
 * @author scisaga@gmail.com
 */
public class Refactor {
	
	public static Class<? extends Annotation> annotationClass = DBName.class;

	/**
	 * 
	 * @param clazz
	 * @throws Exception
	 */
	public static void createTable(Class<? extends Object> clazz) {
		Set<Class<? extends Object>> set = new HashSet<>();
		set.add(clazz);
		createTables(set);
	}
	
	/**
	 * 
	 * @param clazz
	 * @throws Exception
	 */
	public static void dropTable(Class<? extends Object> clazz) {
		Set<Class<? extends Object>> set = new HashSet<>();
		set.add(clazz);
		dropTables(set);
	}

	/**
	 *
	 * @param clazz
	 * @throws DBInitException
	 * @throws SQLException
	 */
	public static void clearTable(Class<? extends Object> clazz) throws DBInitException, SQLException {
		Set<Class<? extends Object>> set = new HashSet<>();
		set.add(clazz);
		clearTables(set);
	}
	
	/**
	 * 
	 * @param packageName
	 * @throws Exception
	 */
	public static void createTables(String packageName) {
		createTables(getClasses(packageName));
	}
	
	/**
	 * 
	 * @param packageName
	 * @throws Exception
	 */
	public static void dropTables(String packageName) {

		dropTables(getClasses(packageName));
	}

	/**
	 *
	 * @param packageName
	 * @throws DBInitException
	 * @throws SQLException
	 */
	public static void clearTables(String packageName) throws DBInitException, SQLException {

		clearTables(getClasses(packageName));
	}

	/**
	 * get model class
	 * 
	 * @throws Exception
	 */
	public static Set<Class<? extends Object>> getClasses(String packageName) {
		Reflections reflections = new Reflections(packageName);
		return reflections.getTypesAnnotatedWith(annotationClass);
	}
	
	/**
	 * 
	 * @param classes
	 */
	public static void createTables(Set<Class<? extends Object>> classes) {
		
		for (Class<?> clazz : classes) {
			
			Model.logger.trace("Creating {}...", clazz.getName());
			
			ConnectionSource source;
			
			try {

				String dbName = clazz.getAnnotation(DBName.class).value();
				source = new DataSourceConnectionSource(
					PooledDataSource.getDataSource(dbName),
					PooledDataSource.getDataSource(dbName).getJdbcUrl()
				);
				
				Dao<?, Object> dao = Daos.get(clazz);
				if(!dao.isTableExists()){
					TableUtils.createTable(source, clazz);
					Model.logger.trace("Created {}.", clazz.getName());
				} else {
					Model.logger.info("Table {} already exists.", clazz.getName());
				}
				
			} catch (Exception e) {
				Model.logger.error("Error create table for {}.", clazz.getName(), e);
			}
		}
	}

	/**
	 *
	 * @param classes
	 * @throws SQLException
	 */
	public static void dropTables(Set<Class<? extends Object>> classes) {

		for (Class<?> clazz : classes) {

			Model.logger.trace("Dropping {}...", clazz.getName());

			ConnectionSource source;

			try {

				String dbName = clazz.getAnnotation(DBName.class).value();

				source = new DataSourceConnectionSource(
						PooledDataSource.getDataSource(dbName),
						PooledDataSource.getDataSource(dbName).getJdbcUrl()
				);

				TableUtils.dropTable(source, clazz, true);
				Model.logger.trace("Dropped {}.", clazz.getName());

			}
			catch (Exception e) {
				Model.logger.error("Error drop table for {}.", clazz.getName(), e);
			}
		}
	}

	/**
	 *
	 * @param classes
	 * @throws DBInitException
	 * @throws SQLException
	 */
	public static void clearTables(Set<Class<? extends Object>> classes) throws DBInitException, SQLException {

		for (Class<?> clazz : classes) {

			Model.logger.trace("Clearing {}...", clazz.getName());

			String dbName = clazz.getAnnotation(DBName.class).value();

			ConnectionSource source = new DataSourceConnectionSource(
					PooledDataSource.getDataSource(dbName),
					PooledDataSource.getDataSource(dbName).getJdbcUrl()
			);

			TableUtils.clearTable(source, clazz);
			Model.logger.trace("Cleared {}.", clazz.getName());
		}
	}

}
