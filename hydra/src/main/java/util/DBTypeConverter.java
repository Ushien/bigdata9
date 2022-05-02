package util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.UUID;

import javax.sql.rowset.serial.SerialBlob;

public class DBTypeConverter {
	
	protected static String getString(Object o) {
		return o.toString();
	}

	protected static Short getShort(Object o) {
		if(o instanceof Short)
			return (Short) o;
		if (o instanceof Integer)
			return ((Integer) o).shortValue();
		if (o instanceof Long)
			return ((Long) o).shortValue();
		if (o instanceof Boolean)
			return (boolean) o ? (short) 1 : (short) 0;
		if (o instanceof Float)
			return ((Float) o).shortValue();
		if (o instanceof Double)
			return ((Double) o).shortValue();

		try {
			return Short.parseShort(o.toString());
		} catch (NumberFormatException e) {
			return null;
		}

	}

	protected static UUID getUUID(Object o) {
		try {
			return UUID.fromString(o.toString());
		} catch (IllegalArgumentException e) {
			return null;
		}
	}

	protected static Integer getInteger(Object o) {
		if (o instanceof Integer)
			return ((Integer) o);
		if (o instanceof Short)
			return ((Short) o).intValue();
		if (o instanceof Long)
			return ((Long) o).intValue();
		if (o instanceof Boolean)
			return (boolean) o ? 1 : 0;
		if (o instanceof Float)
			return ((Float) o).intValue();
		if (o instanceof Double)
			return ((Double) o).intValue();

		try {
			return Integer.parseInt(o.toString());
		} catch (NumberFormatException e) {
			return null;
		}
	}
	
	protected static Long getLong(Object o) {
		if(o instanceof Long)
			return (Long) o;
		if (o instanceof Integer)
			return ((Integer) o).longValue();
		if (o instanceof Short)
			return ((Short) o).longValue();
		if (o instanceof Long)
			return ((Long) o).longValue();
		if (o instanceof Boolean)
			return (boolean) o ? 1L : 0L;
		if (o instanceof Float)
			return ((Float) o).longValue();
		if (o instanceof Double)
			return ((Double) o).longValue();

		try {
			return Long.parseLong(o.toString());
		} catch (NumberFormatException e) {
			return null;
		}
	}

	protected static LocalTime getLocalTime(Object o) {
		//TODO pml::LocalTime is missing
		return null;
	}

	protected static Instant getInstant(Object o) {
		if(o instanceof LocalDateTime) // yyyy-mm-dd[(T| )HH:MM:SS[.fff]][(+|-)NNNN]
			return ((LocalDateTime) o).atZone(ZoneId.systemDefault()).toInstant();
		
		if(o instanceof LocalDate) // yyyy-mm-dd
			return ((LocalDate) o).atStartOfDay().atZone(ZoneId.systemDefault()).toInstant();
		
		try {
			return LocalDateTime.parse(o.toString(), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")).atZone(ZoneId.systemDefault()).toInstant();
		} catch(DateTimeParseException e) {
			return null;
		}
		
	}
	
	protected static LocalDateTime getLocalDateTime(Object o) {
		if(o instanceof LocalDateTime) // yyyy-mm-dd[(T| )HH:MM:SS[.fff]][(+|-)NNNN]
			return (LocalDateTime) o;
		
		if(o instanceof LocalDate) // yyyy-mm-dd
			return ((LocalDate) o).atStartOfDay();
		
		try {
			return LocalDateTime.parse(o.toString(), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
		} catch(DateTimeParseException e) {
			return null;
		}
		
	}
	
	protected static Date getDate(Object o) {
		if(o instanceof LocalDate) // yyyy-mm-dd
			return Date.valueOf((LocalDate) o);
		
		if(o instanceof LocalDateTime) // yyyy-mm-dd[(T| )HH:MM:SS[.fff]][(+|-)NNNN]
			return Date.valueOf(((LocalDateTime) o).toLocalDate());
		
		try {
			return Date.valueOf(LocalDate.parse(o.toString(), DateTimeFormatter.ofPattern("yyyy-MM-dd")));
		} catch(DateTimeParseException e) {
			return null;
		}
	}
	
	protected static Time getTime(Object o) {
		//TODO pml::LocalTime is missing
		return null;
	}
	
	protected static Timestamp getTimestamp(Object o) {
		if(o instanceof LocalDate) // yyyy-mm-dd
			return Timestamp.valueOf(((LocalDate) o).atStartOfDay());
		
		if(o instanceof LocalDateTime) // yyyy-mm-dd[(T| )HH:MM:SS[.fff]][(+|-)NNNN]
			return Timestamp.valueOf((LocalDateTime) o);
		
		try {
			return Timestamp.valueOf(LocalDateTime.parse(o.toString(), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
		} catch(DateTimeParseException e) {
			return null;
		}
	}

	protected static LocalDate getLocalDate(Object o) {
		if(o instanceof LocalDate) // yyyy-mm-dd
			return (LocalDate) o;
		
		if(o instanceof LocalDateTime) // yyyy-mm-dd[(T| )HH:MM:SS[.fff]][(+|-)NNNN]
			return ((LocalDateTime) o).toLocalDate();
		
		try {
			return LocalDate.parse(o.toString(), DateTimeFormatter.ofPattern("yyyy-MM-dd"));
		} catch(DateTimeParseException e) {
			return null;
		}
	}

	protected static Float getFloat(Object o) {
		if (o instanceof Float)
			return ((Float) o);
		if (o instanceof Integer)
			return ((Integer) o).floatValue();
		if (o instanceof Short)
			return ((Short) o).floatValue();
		if (o instanceof Long)
			return ((Long) o).floatValue();
		if (o instanceof Boolean)
			return (boolean) o ? 1f : 0f;
		if (o instanceof Double)
			return ((Double) o).floatValue();

		try {
			return Float.parseFloat(o.toString());
		} catch (NumberFormatException e) {
			return null;
		}
	}

	protected static Double getDouble(Object o) {
		if (o instanceof Float)
			return ((Float) o).doubleValue();
		if (o instanceof Integer)
			return ((Integer) o).doubleValue();
		if (o instanceof Short)
			return ((Short) o).doubleValue();
		if (o instanceof Long)
			return ((Long) o).doubleValue();
		if (o instanceof Boolean)
			return (boolean) o ? 1d : 0d;
		if (o instanceof Double)
			return ((Double) o);

		try {
			return Double.parseDouble(o.toString());
		} catch (NumberFormatException e) {
			return null;
		}
	}

	protected static Byte getByte(Object o) {
		// TODO Auto-generated method stub
		return null;
	}

	protected static Boolean getBoolean(Object o) {
		if(o instanceof Boolean)
			return (Boolean) o;
		
		if(o instanceof String)
			return o.equals("true") || o.equals("1");
		
		if (o instanceof Integer)
			return ((Integer) o) == 1;
		if (o instanceof Short)
			return ((Short) o).intValue() == 1;
		if (o instanceof Long)
			return  ((Long) o).intValue() == 1;
		if (o instanceof Float)
			return  ((Float) o).intValue() == 1;
		if (o instanceof Double)
			return  ((Double) o).intValue() == 1;

		return null;
	}
	

	protected static BigInteger getBigInteger(Object o) {
		if (o instanceof Integer)
			return BigInteger.valueOf((Integer) o);
		if (o instanceof Short)
			return BigInteger.valueOf(((Short) o));
		if (o instanceof Long)
			return  BigInteger.valueOf((Long) o);
		if (o instanceof Boolean)
			return  BigInteger.valueOf((boolean) o ? 1 : 0);
		if (o instanceof Float)
			return  BigInteger.valueOf(((Float) o).intValue());
		if (o instanceof Double)
			return  BigInteger.valueOf(((Double) o).intValue());

		try {
			return BigInteger.valueOf(Integer.parseInt(o.toString()));
		} catch (NumberFormatException e) {
			return null;
		}
	}

	protected static BigDecimal getBigDecimal(Object o) {
		if (o instanceof Integer)
			return new BigDecimal((Integer) o);
		if (o instanceof Short)
			return new BigDecimal(((Short) o));
		if (o instanceof Long)
			return  new BigDecimal((Long) o);
		if (o instanceof Boolean)
			return  new BigDecimal((boolean) o ? 1 : 0);
		if (o instanceof Float)
			return  new BigDecimal(((Float) o).intValue());
		if (o instanceof Double)
			return  new BigDecimal(((Double) o).intValue());

		try {
			return new BigDecimal(o.toString());
		} catch (NumberFormatException e) {
			return null;
		}
	}
	

	protected static Blob getBlob(Object o) {
		try {
			return new SerialBlob(o.toString().getBytes());
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

}

