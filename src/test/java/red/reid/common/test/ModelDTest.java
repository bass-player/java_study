package red.reid.common.test;

import com.j256.ormlite.field.DataType;
import com.j256.ormlite.field.DatabaseField;
import com.j256.ormlite.misc.TransactionManager;
import com.j256.ormlite.stmt.QueryBuilder;
import com.j256.ormlite.table.DatabaseTable;
import one.rewind.db.Daos;
import one.rewind.db.annotation.DBName;
import one.rewind.db.exception.DBInitException;
import one.rewind.db.model.Model;
import one.rewind.db.model.ModelD;
import one.rewind.db.model.ModelL;
import one.rewind.db.util.Refactor;
import one.rewind.json.JSON;
import one.rewind.util.DateFormatUtil;
import one.rewind.util.StringUtil;
import org.junit.Test;

import java.sql.SQLException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * https://ormlite.com/javadoc/ormlite-core/doc-files/ormlite_1.html#Getting-Started
 */
public class ModelDTest {

	@Test
	public void createTables() {
		Refactor.dropTable(ModelA.class);
		Refactor.createTable(ModelA.class);
		Refactor.dropTable(ModelB.class);
		Refactor.createTable(ModelB.class);
		Refactor.dropTable(ModelC.class);
		Refactor.createTable(ModelC.class);
	}

	/**
	 * \' => '
	 * \\' => \"
	 * @throws DBInitException
	 * @throws SQLException
	 */
	@Test
	public void escapeTest() throws DBInitException, SQLException {
		createTables();

		ModelA ma = new ModelA();
		ma.id = "1";
		ma.text = "\\\\\\\\\\'";
		System.err.println(ma.text);
		System.err.println(ma.toInsertSql());
		Model.batchInsert(Arrays.asList(ma));
	}

	@Test
	public void inspectFields() {

		List<ModelA> ms = new ArrayList<>();
		for(int i=0; i<10000; i++) {
			ms.add(new ModelA(i));
		}
		long t1 = System.currentTimeMillis();
		Model.batchInsert(ms);
		System.err.println(System.currentTimeMillis() - t1);
	}

	@Test
	public void testInsert() throws DBInitException, SQLException {

		ModelC mc = new ModelC();
		mc.id = StringUtil.md5("test_3");
		mc.text = "test";
		mc.insert();

		/*String dbName = "raw";

		ConnectionSource source = new DataSourceConnectionSource(PooledDataSource.getDataSource(dbName), PooledDataSource.getDataSource(dbName).getJdbcUrl());
		Dao<ModelC, Object> dao = DaoManager.createDao(source, ModelC.class);

		ModelC mc_ = dao.queryForId(mc.id);
		System.out.println(mc_.toJSON());*/

		ModelC mc_ = /*Daos.get(ModelC.class).queryForId(mc.id)*/ ModelC.getById(ModelC.class, mc.id);
		System.out.println(mc_.toJSON());
	}

	@DBName(value = "raw")
	@DatabaseTable(tableName = "model_as")
	public static class ModelA extends ModelD {

		@DatabaseField(dataType = DataType.INTEGER)
		public int platform_id;

		@DatabaseField(dataType = DataType.STRING, width = 64)
		public String media_id;

		@DatabaseField(dataType = DataType.STRING, width = 64)
		public String media_nick;

		@DatabaseField(dataType = DataType.STRING, width = 36)
		public String text;

		public ModelA() {}

		public ModelA(int platform_id) {
			this.platform_id = platform_id;
			this.text = UUID.randomUUID().toString();
			this.id = StringUtil.md5(this.text);
			this.media_nick = "";
		}
	}

	@DBName(value = "raw")
	@DatabaseTable(tableName = "model_bs")
	public static class ModelB extends ModelL {

		@DatabaseField(dataType = DataType.INTEGER, canBeNull = false, index = true)
		public int num = 0;

		@DatabaseField(columnName = "ma_id", canBeNull = false, foreign = true, foreignAutoRefresh = true)
		public ModelA ma;

		@Test
		public void testInsertSql() throws DBInitException, SQLException {

			/*ModelA ma = new ModelA(0);
			ma.text = "Text";
			ma.media_id = "FFFF";
			ma.insert();*/

			List<ModelA> ms = new ArrayList<>();
			for(int i=0; i<100; i++) {
				ModelA ma = new ModelA(i);
				ms.add(ma);
			}

			Model.batchInsert(ms);
		}

		@Test
		public void createTables() throws DBInitException, SQLException {

			Refactor.dropTable(ModelB.class);
			Refactor.dropTable(ModelA.class);

			Refactor.createTable(ModelB.class);
			Refactor.createTable(ModelA.class);

			for(int i=0; i<10; i++) {

				ModelA ma = new ModelA(i);
				ma.insert();

				ModelB mb = new ModelB();
				mb.ma = ma;
				mb.insert();
			}
		}

		@Test
		public void testQuery0() throws DBInitException, SQLException, InterruptedException {

			// https://ormlite.com/javadoc/ormlite-core/doc-files/ormlite_3.html#Statement-Builder
			QueryBuilder<ModelA, Object> qb = Daos.get(ModelA.class).queryBuilder();

			qb.where().gt("update_time", DateFormatUtil.parseTime("2020-10-31 19:55:33")).and().le("update_time", DateFormatUtil.parseTime("2020-10-31 19:55:34"));

			qb.query().stream().forEach(m -> {
				System.err.println(m.toJSON());
			});
		}

		@Test
		public void testQuery() throws DBInitException, SQLException, InterruptedException {

			QueryBuilder<ModelB, Object> qb = Daos.get(ModelB.class).queryBuilder();
			QueryBuilder<ModelA, Object> qa = Daos.get(ModelA.class).queryBuilder();

			qa.setWhere(qa.where().eq("platform_id", 1));
			qb.setWhere(qb.where().eq("num", 0));

			List<ModelB> mbs = qb.join(qa).query();

			mbs.stream().forEach(mb -> {
				System.err.println(JSON.toPrettyJson(mb));
			});
		}

		@Test
		public void testTransactions() {

			try {
				TransactionManager.callInTransaction(
					Daos.get(ModelB.class).getConnectionSource(),
					(Callable<Void>) () -> {

						ModelA ma = new ModelA(1);
						ma.insert();

						ModelB mb = new ModelB();
						mb.ma = ma;
						mb.insert();
						return null;
					}
				);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	@DBName(value = "raw")
	@DatabaseTable(tableName = "model_cs")
	public static class ModelC extends ModelD {

		public ModelC() {}

		public ModelC(String id, String text) {

			this.id = id;
			this.text = text;
		}

		@DatabaseField(dataType = DataType.STRING, width = 36, canBeNull = false, index = true)
		public String text;

	}
}
