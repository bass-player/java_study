package one.rewind.db.model;

import com.j256.ormlite.field.DataType;
import com.j256.ormlite.field.DatabaseField;

public abstract class ModelL extends Model<ModelL> {

	@DatabaseField(dataType = DataType.INTEGER, generatedId = true)
	public int id;

	public ModelL setId(Integer id) {
		this.id = id;
		return this;
	}

	public Integer getId() {
		return this.id;
	}
}
