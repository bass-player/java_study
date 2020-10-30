package one.rewind.db.model;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.field.DataType;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.table.DatabaseTableConfig;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import one.rewind.db.Daos;
import one.rewind.db.PooledDataSource;
import one.rewind.db.annotation.DBName;
import one.rewind.db.exception.DBInitException;
import one.rewind.json.JSON;
import one.rewind.json.JSONable;
import one.rewind.util.DateFormatUtil;
import one.rewind.util.StringUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

/**
 *
 */
public abstract class Model<T> implements JSONable<T> {

	public static final Logger logger = LogManager.getLogger(Model.class.getName());

	public enum RDBMS {
		MySQL,
		CrDB
	}

	//public static Kryo kryo = new Kryo();

	public interface FieldSerializer<T> {
		String serialize(T o);
	}

	public static Map<Class<? extends Model>, List<String>> ModelInsertFields = new HashMap<>();

	// 字段转化为SQL的序列化方法 TODO 使用Class<?>作为key有潜在风险，应使用String类型
	private static Map<String, FieldSerializer> FieldSerializerMap = new HashMap<>();

	public static <T> void setFieldSerializer(String clazz, FieldSerializer<T> fs) {
		FieldSerializerMap.put(clazz, fs);
	}

	public static <T> void setFieldSerializer(Map<String, FieldSerializer<T>> map) {
		map.forEach(Model::setFieldSerializer);
	}

	/**
	 *
	 * @param clazz
	 * @param insertFields
	 */
	public static void setInsertField(Class<? extends Model> clazz, List<String> insertFields) {
		ModelInsertFields.put(clazz, insertFields);
	}

	/**
	 *
	 * @param clazz
	 * @return
	 */
	public static List<String> getInsertFields(Class<? extends Model> clazz) {
		if(ModelInsertFields.containsKey(clazz)) return ModelInsertFields.get(clazz);
		return new ArrayList<>();
	}

	/**
	 * 每个线程的 Kryo 实例
	 * 参考：https://www.cnblogs.com/hntyzgn/p/7122709.html
	 */
	private static final ThreadLocal<Kryo> kryoLocal = ThreadLocal.withInitial(() -> {

		Kryo kryo = new Kryo();

		//SynchronizedCollectionsSerializer.registerSerializers(kryo);
		UnmodifiableCollectionsSerializer.registerSerializers(kryo);

		/**
		 * 不要轻易改变这里的配置！更改之后，序列化的格式就会发生变化，
		 * 上线的同时就必须清除 Redis 里的所有缓存，
		 * 否则那些缓存再回来反序列化的时候，就会报错
		 */
		//支持对象循环引用（否则会栈溢出）
		kryo.setReferences(true); //默认值就是 true，添加此行的目的是为了提醒维护者，不要改变这个配置

		//不强制要求注册类（注册行为无法保证多个 JVM 内同一个类的注册编号相同；而且业务系统中大量的 Class 也难以一一注册）
		kryo.setRegistrationRequired(false); //默认值就是 false，添加此行的目的是为了提醒维护者，不要改变这个配置

		return kryo;
	});

	/**
	 * 获得当前线程的 Kryo 实例
	 *
	 * @return 当前线程的 Kryo 实例
	 */
	public static Kryo getInstance() {
		return kryoLocal.get();
	}

	@DatabaseField(dataType = DataType.DATE, width = 3)
	public Date create_time = new Date();

	@DatabaseField(dataType = DataType.DATE, width = 3, indexName = "default")
	public Date update_time = new Date();

	public Model() {}

	/**
	 * @return
	 * @throws Exception
	 */
	public boolean insert() throws DBInitException, SQLException {

		Dao dao = Daos.get(this.getClass());

		update_time = new Date();

		if (dao.create(this) == 1) {
			return true;
		}

		return false;
	}

	/**
	 *
	 * @param update_time
	 * @return
	 * @throws DBInitException
	 * @throws SQLException
	 */
	public boolean insert(Date update_time) throws DBInitException, SQLException {

		Dao dao = Daos.get(this.getClass());

		this.update_time = update_time;

		if (dao.create(this) == 1) {
			return true;
		}

		return false;
	}

	/**
	 * 更新数据
	 *
	 * @return
	 * @throws Exception
	 */
	public boolean update() throws DBInitException, SQLException {

		Dao dao = Daos.get(this.getClass());

		update_time = new Date();

		if (dao.update(this) == 1) {
			return true;
		}

		return false;
	}

	/**
	 * @param clazz
	 * @param id
	 * @return
	 * @throws Exception
	 */
	public static <T> T getById(Class<T> clazz, String id) throws DBInitException, SQLException {

		Dao dao = Daos.get(clazz);

		return (T) dao.queryForId(id);
	}

	/**
	 * @param clazz
	 * @param id
	 * @param <T>
	 * @return
	 * @throws DBInitException
	 * @throws SQLException
	 */
	public static <T> T getById(Class<T> clazz, int id) throws DBInitException, SQLException {

		Dao dao = Daos.get(clazz);

		return (T) dao.queryForId(id);
	}

	/**
	 * @param clazz
	 * @return
	 * @throws Exception
	 */
	public static <T> List<T> getAll(Class<T> clazz) throws DBInitException, SQLException {

		Dao dao = Daos.get(clazz);

		return (List<T>) dao.queryForAll();
	}

	/**
	 * @param id
	 * @throws Exception
	 */
	public static <T> void deleteById(Class<T> clazz, String id) throws DBInitException, SQLException {
		Dao<T, Object> dao = Daos.get(clazz);
		dao.deleteById(id);
	}

	@Override
	public String toJSON() {
		return JSON.toJson(this);
	}

	/**
	 *
	 * @param jsonStr
	 * @param clazz
	 * @param <T>
	 * @return
	 */
	public static <T> T fromJSON(String jsonStr, Class<? extends Model> clazz) {
		return (T) JSON.fromJson(jsonStr, clazz);
	}

	/**
	 *
	 * @param obj
	 * @return
	 */
	public static byte[] toBytes(Object obj) {

		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		Output output = new Output(byteArrayOutputStream);

		Kryo kryo = Model.getInstance();
		kryo.writeClassAndObject(output, obj);
		output.flush();

		return byteArrayOutputStream.toByteArray();
	}

	/**
	 * @return
	 * @throws Exception
	 */
	public byte[] toBytes() {

		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		Output output = new Output(byteArrayOutputStream);

		Kryo kryo = getInstance();
		kryo.writeClassAndObject(output, this);
		output.flush();

		return byteArrayOutputStream.toByteArray();
	}

	/**
	 *
	 * @param bytes
	 * @param <T>
	 * @return
	 */
	public static <T> T fromBytes(byte[] bytes) {

		if (bytes == null || bytes.length == 0) return null;

		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
		Input input = new Input(byteArrayInputStream);

		Kryo kryo = getInstance();
		return (T) kryo.readClassAndObject(input);
	}

	/**
	 *
	 * @return
	 */
	public String toInsertSql() {

		Field[] fields = this.getClass().getFields();

		return "(" + Arrays.stream(fields).map(f -> {

			DatabaseField df = f.getAnnotation(DatabaseField.class);
			if(df != null && !df.foreign()) {

				if(this instanceof ModelL && f.getName().equals("id")) return null;

				try {
					Class type = f.getType();

					if(f.get(this) == null) return "NULL";

					if (type == String.class) {

						return "'"
							+ f.get(this).toString()
								.replaceAll("\\\\{2,1000}","\\\\")
								.replaceAll("\\\\?'","\\\\'")
							+ "'";
					}
					else if (Enum.class.isAssignableFrom(type)) {
						return "'" + f.get(this) + "'";
					}
					else if (type == Date.class) {
						return "'" + DateFormatUtil.dfff.print(((Date) f.get(this)).getTime()) + "'";
					}
					else if (type == byte[].class && f.getName().matches(".*?id")) {
						return "'" + StringUtil.byteArrayToHex((byte[]) f.get(this)) + "'";
					}
					else if (Map.class.isAssignableFrom(type) || List.class.isAssignableFrom(type)) {
						return "'" + JSON.toJson(f.get(this)) + "'";
					}
					else if(FieldSerializerMap.containsKey(type.getName())) {
						return FieldSerializerMap.get(type.getName()).serialize(f.get(this));
					}
					else {
						return f.get(this).toString();
					}
				} catch (Exception e) {
					logger.error("Error get field[{}]", f.getName(), e);
				}
			}
			return null;

		}).filter(Objects::nonNull).collect(Collectors.joining(",")) + ")";
	}

	/**
	 * 生成批量插入的字段
	 * @param m
	 * @return
	 */
	private static String getInsertFields(Model m, RDBMS dbType) {

		Field[] fields = m.getClass().getFields();

		return "(" + Arrays.stream(fields).map(f -> {

			DatabaseField df = f.getAnnotation(DatabaseField.class);

			if(df != null && !df.foreign()) {
				if(m instanceof ModelL && f.getName().equals("id")) return null;

				String field = f.getName();
				if(field.matches("like|collect|index")) {
					return dbType == RDBMS.CrDB ?  ("\"" + field + "\"") : ("`" + field + "`");
				}
				else {
					return f.getName();
				}
			}
			return null;

		}).filter(Objects::nonNull).collect(Collectors.joining(",")) + ")";
	}

    /**
     *
     * @param models
     * @param partitionName 分区名
     * @param partitionValue 分区值
     * @return
     */
	public static String toHiveSql(List<? extends Model> models, String partitionName, String partitionValue) {

		if(models.size() > 0) {

			Class<? extends Model> clazz = models.get(0).getClass();
			String dbName = null;
			try {

				dbName = clazz.getAnnotation(DBName.class).value();

			} catch (Exception e){
				logger.error("Error get dbName annotation for {}.", clazz.getName(), e);
			}

			String tableName = DatabaseTableConfig.extractTableName(clazz);

			Field[] declaredFields = clazz.getDeclaredFields();

			if(getInsertFields(clazz).size() == 0) {

				List<String> insertFields = new ArrayList<>();

				for (int i = 0; i < declaredFields.length; i++) {
					insertFields.add(declaredFields[i].getName());
				}

				setInsertField(clazz, insertFields);
			}

			String sql = "INSERT INTO " + tableName + " partition("+ partitionName + "=" + partitionValue + ")" + " VALUES ";

			sql += models.stream().map(m -> m.toInsertSql()).collect(Collectors.joining(","));

			return sql;
		}

		return null;
	}

	/**
	 * 常用的的Mysql的批量插入方法, ModelD可能因为主键冲突而保存失败
	 * @param models
	 */
	public static void batchInsert(List<? extends Model> models) {
		batchInsert(models, RDBMS.MySQL, null);
	}

	/**
	 * 常用的的Mysql的批量插入方法, 不会因为主键冲突而保存失败, 冲突的数据舍弃
	 * @param models
	 */
	public static void batchInsertIgnore(List<? extends ModelD> models) {
		batchInsert(models, RDBMS.MySQL, "UPDATE id = id");
	}

	/**
	 * 加入了存储类型的额外处理, CrDB常用的批量插入方法, ModelD可能因为主键冲突而保存失败
	 * @param models
	 * @param dbType
	 */
	public static void batchInsert(List<? extends Model> models, RDBMS dbType) {
		batchInsert(models, dbType, null);
	}

	/**
	 * 基础批量插入方法
	 * @param models 批量插入的ModelList
	 * @param dbType 存储类型
	 * @param onDuplicateKeyExpr 补充主键冲突时更新的语句
	 */
	private static void batchInsert(List<? extends Model> models, RDBMS dbType, String onDuplicateKeyExpr) {

		if(models.size() > 0) {

			Class<? extends Model> clazz = models.get(0).getClass();
			String dbName = null;
			try {

				dbName = clazz.getAnnotation(DBName.class).value();

			} catch (Exception e){
				logger.error("Error get dbName annotation for {}.", clazz.getName(), e);
			}

			String tableName = DatabaseTableConfig.extractTableName(clazz);

			Field[] declaredFields = clazz.getDeclaredFields();

			if(getInsertFields(clazz).size() == 0) {

				List<String> insertFields = new ArrayList<>();

				for (int i = 0; i < declaredFields.length; i++) {
					insertFields.add(declaredFields[i].getName());
				}

				setInsertField(clazz, insertFields);
			}

			String sql = "INSERT INTO " + tableName + " " + getInsertFields(models.get(0), dbType) + " VALUES ";

			sql += models.stream().map(Model::toInsertSql).collect(Collectors.joining(","))
				+ (onDuplicateKeyExpr != null ? (" ON DUPLICATE KEY " + onDuplicateKeyExpr) : "") +  ";";

			//logger.info("SQL --> {}", sql);

			try {

				Connection conn = PooledDataSource.getDataSource(dbName).getConnection();
				Statement stmt = conn.createStatement();

				try {
					stmt.execute(sql);
				} catch (SQLException e) {
					logger.error("Error insert {}, {}, ", tableName, sql, e);
				}

				stmt.close();
				conn.close();
			} catch (Exception e) {
				logger.error("Error get connection {}, {}, ", dbName, tableName, e);
			}
		}
	}

	/**
	 * Mysql批量插入更新, 如果主键冲突了以最后一条数据为准, 效果如同删除之前的数据增加新数据, 不会报主键冲突的错
	 * @param models
	 * @param onDuplicateKeyExpr 补充主键冲突时更新语句
	 */
	public static void batchUpsert(List<? extends ModelD> models, String onDuplicateKeyExpr) {
		batchInsert(models, RDBMS.MySQL, onDuplicateKeyExpr);
	}

	/**
	 * CrDB批量插入更新, 如果主键冲突了以最后一条数据为准, 效果如同删除之前的数据增加新数据, 不会报主键冲突的错
	 * @param models
	 */
	public static void batchUpsert(List<? extends ModelD> models) {

		if(models.size() > 0) {

			Class<? extends Model> clazz = models.get(0).getClass();
			String dbName = null;
			try {

				dbName = clazz.getAnnotation(DBName.class).value();

			} catch (Exception e){
				logger.error("Error get dbName annotation for {}.", clazz.getName(), e);
			}

			String tableName = DatabaseTableConfig.extractTableName(clazz);

			Field[] declaredFields = clazz.getDeclaredFields();

			if(getInsertFields(clazz).size() == 0) {

				List<String> insertFields = new ArrayList<>();

				for (int i = 0; i < declaredFields.length; i++) {
					insertFields.add(declaredFields[i].getName());
				}

				setInsertField(clazz, insertFields);
			}

			String sql = "UPSERT INTO " + tableName + " " + getInsertFields(models.get(0), RDBMS.CrDB) + " VALUES ";

			sql += models.stream().map(m -> m.toInsertSql()).collect(Collectors.joining(",")) + ";";

			//logger.info("SQL --> {}", sql);

			try {

				Connection conn = PooledDataSource.getDataSource(dbName).getConnection();
				Statement stmt = conn.createStatement();

				try {
					stmt.execute(sql);
				} catch (SQLException e) {
					logger.error("Error insert {}, {}, ", tableName, sql, e);
				}

				stmt.close();
				conn.close();
			} catch (Exception e) {
				logger.error("Error get connection {}, {}, ", dbName, tableName, e);
			}
		}
	}

	/**
	 * 生成Mysql batchUpsert需要更新的onDuplicateKeyExpr
	 * @param onDuplicateKeyExprList
	 * @return
	 */
	public static String toUpdateString(List<String> onDuplicateKeyExprList) {

		String update = null;

		if (onDuplicateKeyExprList != null && onDuplicateKeyExprList.size() > 0) {
			update = "UPDATE " + onDuplicateKeyExprList.stream().map(each -> each + " = VALUES(" + each + ")").collect(Collectors.joining(","));
		}

		return update;
	}
}
