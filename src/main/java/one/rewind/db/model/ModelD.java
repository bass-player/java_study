package one.rewind.db.model;

import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.field.DataType;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.field.FieldType;
import com.j256.ormlite.field.SqlType;
import com.j256.ormlite.field.types.ByteArrayType;
import com.j256.ormlite.field.types.StringType;
import com.j256.ormlite.support.DatabaseResults;
import one.rewind.db.Daos;
import one.rewind.db.exception.DBInitException;
import one.rewind.db.exception.ModelException;
import one.rewind.util.StringUtil;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

/**
 * 只有ModelD 存在版本管理概念
 */
public abstract class ModelD extends Model<ModelD> {

	// ID 字段
	@DatabaseField(dataType = DataType.STRING, width = 64, id = true)
	public String id;

	/**
	 *
	 * @param clazz
	 * @param id
	 * @param <T>
	 * @throws DBInitException
	 * @throws SQLException
	 */
	public static <T> void deleteById(Class<T> clazz, byte[] id) throws DBInitException, SQLException {
		Dao<T, Object> dao = Daos.get(clazz);
		dao.deleteById(id);
	}

	/**
	 *
	 * @param id
	 * @return
	 */
	public ModelD setId(String id) {
		this.id = id;
		return this;
	}

	/**
	 *
	 * @return
	 */
	public String getId() {
		return this.id;
	}

	/**
	 * 插入更新方法
	 * @return
	 * @throws DBInitException
	 * @throws SQLException
	 * @throws ModelException.ClassNotEqual
	 * @throws IllegalAccessException
	 */
	public boolean upsert() throws DBInitException, SQLException, ModelException.ClassNotEqual, IllegalAccessException {

		Dao dao = Daos.get(this.getClass());

		ModelD oldVersion = (ModelD) dao.queryForId(this.id);

		// 没有旧版本
		if (oldVersion == null) {

			update_time = new Date();

			if (super.insert()) {
				createSnapshot(this); // 第一次采集 也需要创建快照
				return true;
			}
		}
		// 存在旧版本
		else {

			if (diff(oldVersion)) {

				createSnapshot(oldVersion); // 创建快照
				oldVersion.copy(this); // 新值覆盖旧值

				oldVersion.update_time = new Date();
				return oldVersion.update();
			}
		}

		return false;
	}

	/**
	 * 判断内容是否相同
	 * @param model
	 * @return
	 * @throws Exception
	 */
	public boolean diff(Model model) throws ModelException.ClassNotEqual, IllegalAccessException {

		if(!model.getClass().equals(this.getClass())) {
			throw new ModelException.ClassNotEqual();
		}

		Field[] fieldList = model.getClass().getDeclaredFields();

		for(Field f : fieldList) {

			if (!f.getName().equals("insert_time")
					&& !f.getName().equals("update_time")) {

				if(f.get(model) == null && f.get(this) != null || f.get(model) != null && f.get(this) == null)
					return true;

				if(f.get(model) != null && f.get(this) != null && !f.get(model).toString().equals(f.get(this).toString()) ){
					return true;
				}
			}
		}

		return false;
	}

	/**
	 * 将model中的字段拷贝到this
	 * @param model
	 * @throws Exception
	 */
	public void copy(Model model) throws ModelException.ClassNotEqual {

		if(!model.getClass().equals(this.getClass())) {
			throw new ModelException.ClassNotEqual();
		}

		Field[] fieldList = model.getClass().getDeclaredFields();

		for(Field f : fieldList) {

			try {
				if (f.get(model) != null
						&& !f.getName().equals("insert_time")
						&& !f.getName().equals("update_time")) {

					Field f_ = this.getClass().getField(f.getName());
					f_.set(this, f.get(model));
				}
			} catch (Exception e) {
				logger.error("Error copy model field:{}. ", f.getName(), e);
			}
		}
	}

	/**
	 * 子类重载，写具体的保存快照方法
	 * @param oldVersion
	 * @throws Exception
	 */
	public void createSnapshot(Model oldVersion) {

	}

	/**
	 * 用于将 byte[] 变量 保存为 Blob 字段
	 */
	public static class PreferByteArrayType extends ByteArrayType {

		public PreferByteArrayType() {
			super(SqlType.BYTE_ARRAY, new Class[] { byte[].class });
		}

		private static final PreferByteArrayType singleTon = new PreferByteArrayType();

		public static PreferByteArrayType getSingleton() {
			return singleTon;
		}

		@Override
		public boolean isAppropriateId() {
			return true;
		}
	}

	/**
	 * 将byte[]类型id 保存为varchar类型字段
	 */
	public static class IdPersister extends StringType {

		private static final IdPersister INSTANCE = new IdPersister();

		protected IdPersister() {
			super(SqlType.STRING, new Class<?>[] { List.class });
		}

		public static IdPersister getSingleton() {
			return INSTANCE;
		}

		@Override
		public Object javaToSqlArg(FieldType fieldType, Object javaObject) {

			return StringUtil.byteArrayToHex((byte[]) javaObject);
		}

		@Override
		public Object sqlArgToJava(FieldType fieldType, Object sqlArg, int columnPos) {
			return sqlArg != null ? StringUtil.hexStringToByteArray((String) sqlArg) : null;
		}
	}

	/**
	 * 将byte[]类型id 保存为varchar类型字段
	 */
	public static class KryoPersister extends ByteArrayType {

		private static final KryoPersister INSTANCE = new KryoPersister();

		protected KryoPersister() {
			super(SqlType.BYTE_ARRAY, new Class<?>[0]);
		}

		public static KryoPersister getSingleton() {
			return INSTANCE;
		}

		@Override
		public Object javaToSqlArg(FieldType fieldType, Object javaObject) {

			return Model.toBytes((Serializable) javaObject);
		}

		@Override
		public Object resultToSqlArg(FieldType fieldType, DatabaseResults results, int columnPos) throws SQLException {

			byte[] res = results.getBytes(columnPos);
			return res != null ? Model.fromBytes(res): null;
		}
	}
}
