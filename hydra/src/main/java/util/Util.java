package util;

import conditions.*;
import exceptions.IncompatibleAttributesTypeException;
import java.util.HashSet;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Set;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.time.ZoneId;
import java.util.Date;
import org.bson.types.Binary;

public class Util {
	
	/**
	Les prochaines methodes transformXXXValue ont pour but de transformer 
	l'object de la condition dans un format String conforme � la techno cible
	ex: un boolean doit-il etre traduit en true ou 1?
	ex2: une date, comment la formatter? etc.
	Cela dependra du type de la valeur ainsi que du backend cible
	**/
	public static String transformSQLValue(Object o) {
		if(o == null)
			return null;

		//TODO comportement different en fct des types de donnees(ex: dates et leur format)
		return o.toString();
	}

	public static String transformCQLValue(Object o) {
		if(o == null)
			return null;

		return o.toString();
	}

	public static String transformBSONValue(Object o) {
			if(o == null)
				return null;
			if(o instanceof LocalDate)
				return o.toString().concat("T00:00:00Z");
			//TODO comportement different en fct des types de donnees (ex: dates et leur format)
			return o.toString();
	}

	public static String escapeReservedCharSQL(String v) {
		if(v == null)
			return null;
		//TODO other reserved char in regex sql
		return v.replaceAll("_", "\\\\\\\\_").replaceAll("%", "\\\\\\\\%");
	}

	public static String escapeReservedRegexMongo(String v) {
		if(v == null)
			return null;
		//TODO https://perldoc.perl.org/perlre
		return v.replaceAll("\\*", "\\\\\\\\*").replaceAll("\\^", "\\\\\\\\^").replaceAll("\\$", "\\\\\\\\\\$");
	}

	
	public static String getDelimitedMongoValue(Class cl, String value) {
		if(value == null)
			return null;

		if(cl == String.class)
			return "'" + value.replaceAll("'","\\\\\\'") + "'";
		if(cl == LocalDate.class)
			return "'"+ value + "'";
		//TODO handle the other special data types, e.g., Byte, Date, ...
		return value;
		
	}

	public static String getDelimitedCQLValue(Class cl, String value) {
		if(value == null)
			return null;
		if(cl == null)
			return value;
		if(cl == String.class)
			return "'" + value.replaceAll("'", "\\\\\\\\'") + "'";
		if(cl == LocalDate.class)
			return "'" + value + "'";
		//TODO other data types
		return value;
	}

	public static String getDelimitedSQLValue(Class cl, String value) {
		if(value == null)
			return null;
		if(cl == null)
			return value;
		if(cl == String.class)
			return "'" + value.replaceAll("'", "\\\\\\\\'") + "'";
		if(cl == LocalDate.class)
			return "'" + value + "'";
		//TODO other data types
		return value;
	}

	public static Double getDoubleValue(Object o) {
		if(o==null)
			return null;
		if(o instanceof BigDecimal)
			return ((BigDecimal) o).doubleValue();
		if(o instanceof Float)
			return Double.valueOf(o.toString());
		if(o instanceof Short)
			return ((Short) o).doubleValue();
		if(o instanceof Double)
			return ((Double) o).doubleValue();
		if(o instanceof Integer)
			return ((Integer) o).doubleValue();
		if (o instanceof Long)
			return ((Long) o).doubleValue();
		if (o instanceof String)
			return Double.parseDouble((String) o);
		throw new IncompatibleAttributesTypeException("Conceptual attribute type in model file incompatible with found datatype in database. Double vs "+o.getClass());
	}

	public static Integer getIntegerValue(Object o) {
		if(o==null)
			return null;
		if(o instanceof BigDecimal)
			return ((BigDecimal) o).intValue();
		if(o instanceof Float)
			return ((Float) o).intValue();
		if(o instanceof Double)
			return ((Double) o).intValue();
		if(o instanceof Short)
			return ((Short) o).intValue();
		if(o instanceof Integer)
			return ((Integer) o).intValue();
		if (o instanceof Long)
			return ((Long) o).intValue();
		if(o instanceof String)
			return Integer.parseInt((String) o);
		throw new IncompatibleAttributesTypeException("Conceptual attribute type in model file incompatible with found datatype in database. Integer vs "+o.getClass());
	}

	public static Long getLongValue(Object o) {
		if(o==null)
			return null;
		if(o instanceof BigDecimal)
			return ((BigDecimal) o).longValue();
		if(o instanceof Float)
			return ((Float) o).longValue();
		if(o instanceof Short)
			return ((Short) o).longValue();
		if(o instanceof Double)
			return ((Double) o).longValue();
		if(o instanceof Integer)
			return ((Integer) o).longValue();
		if (o instanceof Long)
			return ((Long) o).longValue();
		if(o instanceof String)
			return Long.parseLong((String)o);
		throw new IncompatibleAttributesTypeException("Conceptual attribute type in model file incompatible with found datatype in database. Long vs "+o.getClass());
	}

	public static String getStringValue(Object o) {
		if(o==null)
			return null;
		if(o instanceof BigDecimal)
			return ((BigDecimal) o).toString();
		if(o instanceof Float)
			return ((Float) o).toString();
		if(o instanceof Double)
			return ((Double) o).toString();
		if(o instanceof Short)
			return ((Short) o).toString();
		if(o instanceof Integer)
			return ((Integer) o).toString();
		if (o instanceof Long)
			return ((Long) o).toString();
		if(o instanceof String)
			return (String) o ;
		return o.toString();
		//throw new IncompatibleAttributesTypeException("Conceptual attribute type in model file incompatible with found datatype in database");
	}

	public static Boolean getBooleanValue(Object o) {
		if(o==null)
			return null;
		if(o instanceof Boolean)
			return (Boolean) o;
		throw new IncompatibleAttributesTypeException("Conceptual attribute type in model file incompatible with found datatype in database. Boolean vs "+o.getClass());
	}

	public static byte[] getByteArrayValue(Object o) {
		if(o==null)
			return null;
		if(o instanceof String)
			return ((String) o).getBytes();
		if (o instanceof byte[]) 
			return (byte[]) o;
		if(o instanceof Binary)
			return ((Binary) o).getData();
		throw new IncompatibleAttributesTypeException("Conceptual attribute type in model file incompatible with found datatype in database. Blob vs "+o.getClass());
	}

	public static LocalDateTime getLocalDateTimeValue(Object o){
		if(o==null)
			return null;
		if(o instanceof LocalDateTime)
			return (LocalDateTime) o;
		if(o instanceof java.sql.Date)
			return LocalDateTime.ofInstant(((java.sql.Date) o).toInstant(), ZoneId.systemDefault());
		if(o instanceof LocalDate)
			return ((LocalDate) o).atStartOfDay();
		if(o instanceof Timestamp)
			return ((Timestamp) o).toLocalDateTime();
		if(o instanceof Date)
			return LocalDateTime.ofInstant(((Date) o).toInstant(), ZoneId.systemDefault());
		if(o instanceof java.time.Instant)
			return LocalDateTime.ofInstant(((java.time.Instant) o), ZoneId.systemDefault());
		try {
			// TODO Fix customizable dateformat
			java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			return LocalDateTime.parse(String.valueOf(o), formatter);
		} catch (java.time.format.DateTimeParseException e) {
			e.printStackTrace();
		}
		throw new IncompatibleAttributesTypeException("Conceptual attribute type in model file incompatible with found datatype in database");
	}

	public static LocalDate getLocalDateValue(Object o){
		if(o==null)
			return null;
		if(o instanceof java.sql.Date)
			return ((java.sql.Date) o).toLocalDate();
		if(o instanceof LocalDate)
			return (LocalDate) o;
		if(o instanceof Timestamp)
			return ((Timestamp) o).toLocalDateTime().toLocalDate();
		if(o instanceof Date)
			return ((Date) o).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
		if(o instanceof java.time.Instant)
			return LocalDate.ofInstant(((java.time.Instant) o), ZoneId.systemDefault());
		try {
			// TODO Fix customizable dateformat
			return new SimpleDateFormat("yyyy-MM-dd").parse(String.valueOf(o)).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		throw new IncompatibleAttributesTypeException("Conceptual attribute type in model file incompatible with found datatype in database. LocalDate vs "+o.getClass());
	}

	public static <E> Object getValueOfAttributeInEqualCondition(Condition<E> condition, E attribute) {
		Object oleft, oright;
		if (condition instanceof SimpleCondition) {
			SimpleCondition cond = ((SimpleCondition<E>) condition);
			if (cond.getOperator() == Operator.EQUALS) {
				if(cond.getAttribute()==attribute)
					return cond.getValue();
			}
		}
		if(condition instanceof AndCondition){
			AndCondition cond = (AndCondition) condition;
			oleft= getValueOfAttributeInEqualCondition(cond.getLeftCondition(), attribute);
			oright= getValueOfAttributeInEqualCondition(cond.getRightCondition(), attribute);
			if(oleft!=null)
				return oleft;
			if(oright!=null)
				return oright;
		}
		if(condition instanceof OrCondition){
			OrCondition cond = (OrCondition) condition;
			oleft= getValueOfAttributeInEqualCondition(cond.getLeftCondition(), attribute);
			oright= getValueOfAttributeInEqualCondition(cond.getRightCondition(), attribute);
			if(oleft!=null)
				return oleft;
			if(oright!=null)
				return oright;
		}
		return null;
	}

	public static boolean containsOrCondition(Condition condition) {
		boolean left=false, right=false;
		AndCondition andCondition;
		if(condition instanceof OrCondition)
			return true;
		if (condition instanceof AndCondition) {
			andCondition = (AndCondition) condition;
			left = containsOrCondition(andCondition.getLeftCondition());
			right = containsOrCondition(andCondition.getRightCondition());
			return left || right;
		}
		return false;
	}

	public static <E> Set<E> getConditionAttributes(Condition<E> condition) {
		Set<E> attributes = new HashSet<>();
		if (condition instanceof SimpleCondition) {
			SimpleCondition simpleCondition = (SimpleCondition) condition;
			attributes.add((E) simpleCondition.getAttribute());
		}
		if (condition instanceof OrCondition) {
			OrCondition orCondition = (OrCondition) condition;
			attributes.addAll(getConditionAttributes(orCondition.getLeftCondition()));
			attributes.addAll(getConditionAttributes(orCondition.getRightCondition()));
		}
		if (condition instanceof AndCondition) {
			AndCondition andCondition = (AndCondition) condition;
			attributes.addAll(getConditionAttributes(andCondition.getLeftCondition()));
			attributes.addAll(getConditionAttributes(andCondition.getRightCondition()));
		}
		return attributes;
	}

}
