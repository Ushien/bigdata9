package util;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public class SQLTypeConverter extends DBTypeConverter {
	
	
	public static Object get(Object o, String className) {
		if (o == null || className == null)
			return null;
		
		
		if(className.equals("java.lang.String"))
			return (String) getString(o);
		if(className.equals("java.lang.Integer"))
			return (Integer) getInteger(o);
		if(className.equals("java.lang.Float"))
			return (Float) getFloat(o);
		if(className.equals("java.sql.Date"))
			return (Date) getDate(o);
		if(className.equals("java.sql.Timestamp"))
			return (Timestamp) getTimestamp(o);
		if(className.equals("java.sql.Time"))
			return (Time) getTime(o);
		if(className.equals("java.math.BigDecimal"))
			return (BigDecimal) getBigDecimal(o);
		if(className.equals("java.lang.Boolean"))
			return (Boolean) getBoolean(o);
		if(className.equals("java.lang.Double"))
			return (Double) getDouble(o);
		if(className.equals("java.lang.Long"))
			return (Long) getLong(o);
		if(className.equals("java.sql.Blob") || className.equals("[B"))
			return (Blob) getBlob(o);
		
//		java.lang.Byte
//		java.sql.Clob
//		java.sql.NClob
		
		return null;
	}

}

