package util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.UUID;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;

public class CassandraTypeConverter extends DBTypeConverter {

	public static Object get(Object o, DataType type) {
		if (o == null || type == null)
			return null;

		if (type.equals(DataTypes.DECIMAL))
			return (BigDecimal) getBigDecimal(o);
		if (type.equals(DataTypes.BIGINT))
			return (BigInteger) getBigInteger(o);
		if (type.equals(DataTypes.BOOLEAN))
			return (Boolean) getBoolean(o);
		if (type.equals(DataTypes.BLOB))
			return (Byte) getByte(o);
		if (type.equals(DataTypes.DOUBLE))
			return (Double) getDouble(o);
		if (type.equals(DataTypes.FLOAT))
			return (Float) getFloat(o);
		if (type.equals(DataTypes.DATE)) // yyyy-mm-dd
			return (LocalDate) getLocalDate(o);
//		if (type.equals(DataTypes.DURATION))
//			return (Instant) getInstant(o);
		if (type.equals(DataTypes.TIMESTAMP)) // yyyy-mm-dd[(T| )HH:MM:SS[.fff]][(+|-)NNNN]
			return (Instant) getInstant(o);
		if (type.equals(DataTypes.TIME)) // HH:MM:SS[.fff]
			return (LocalTime) getLocalTime(o);
		if (type.equals(DataTypes.SMALLINT))
			return (Integer) getInteger(o);
		if (type.equals(DataTypes.UUID))
			return (UUID) getUUID(o);
		if (type.equals(DataTypes.INT))
			return (Integer) getInteger(o);
		if (type.equals(DataTypes.TINYINT))
			return (Short) getShort(o);
		if (type.equals(DataTypes.TEXT))
			return (String) getString(o);
		// java.math.BigDecimal
		// java.math.BigInteger
		// boolean
		// byte
		// double
		// float
		// java.time.Instant
		// int
		// List
		// java.time.LocalDate
		// java.time.LocalTime
		// long
		// Map
		// Set
		// short
		// String
		// java.util.UUID
		return null;
	}

	

}

