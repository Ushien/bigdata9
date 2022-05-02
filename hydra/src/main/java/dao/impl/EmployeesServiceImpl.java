package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Employees;
import conditions.*;
import dao.services.EmployeesService;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import util.Dataset;
import org.apache.spark.sql.Encoders;
import util.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.function.MapFunction;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import com.mongodb.spark.MongoSpark;
import org.bson.Document;
import static java.util.Collections.singletonList;
import dbconnection.SparkConnectionMgr;
import dbconnection.DBConnectionMgr;
import util.WrappedArray;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FilterFunction;
import java.util.ArrayList;
import org.apache.commons.lang.mutable.MutableBoolean;
import tdo.*;
import pojo.*;
import util.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.ArrayType;
import scala.Tuple2;
import org.bson.Document;
import org.bson.conversions.Bson;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;


public class EmployeesServiceImpl extends EmployeesService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EmployeesServiceImpl.class);
	
	
	
	
	public static Pair<String, List<String>> getSQLWhereClauseInEmployeesFromMyRelDB(Condition<EmployeesAttribute> condition, MutableBoolean refilterFlag) {
		return getSQLWhereClauseInEmployeesFromMyRelDBWithTableAlias(condition, refilterFlag, "");
	}
	
	public static List<String> getSQLSetClauseInEmployeesFromMyRelDB(conditions.SetClause<EmployeesAttribute> set) {
		List<String> res = new ArrayList<String>();
		if(set != null) {
			java.util.Map<String, java.util.Map<String, String>> longFieldValues = new java.util.HashMap<String, java.util.Map<String, String>>();
			java.util.Map<EmployeesAttribute, Object> clause = set.getClause();
			for(java.util.Map.Entry<EmployeesAttribute, Object> e : clause.entrySet()) {
				EmployeesAttribute attr = e.getKey();
				Object value = e.getValue();
				if(attr == EmployeesAttribute.employeeID ) {
					res.add("EmployeeID = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.LastName ) {
					res.add("LastName = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.FirstName ) {
					res.add("FirstName = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.Title ) {
					res.add("Title = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.TitleOfCourtesy ) {
					res.add("TitleOfCourtesy = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.BirthDate ) {
					res.add("BirthDate = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.HireDate ) {
					res.add("HireDate = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.Address ) {
					res.add("Address = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.City ) {
					res.add("City = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.Region ) {
					res.add("Region = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.PostalCode ) {
					res.add("PostalCode = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.Country ) {
					res.add("Country = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.HomePhone ) {
					res.add("HomePhone = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.Extension ) {
					res.add("Extension = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.Photo ) {
					res.add("Photo = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.Notes ) {
					res.add("Notes = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.PhotoPath ) {
					res.add("PhotoPath = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeesAttribute.Salary ) {
					res.add("Salary = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
			}
	
			for(java.util.Map.Entry<String, java.util.Map<String, String>> entry : longFieldValues.entrySet()) {
				String longField = entry.getKey();
				java.util.Map<String, String> values = entry.getValue();
			}
	
		}
		return res;
	}
	
	public static Pair<String, List<String>> getSQLWhereClauseInEmployeesFromMyRelDBWithTableAlias(Condition<EmployeesAttribute> condition, MutableBoolean refilterFlag, String tableAlias) {
		String where = null;	
		List<String> preparedValues = new java.util.ArrayList<String>();
		if(condition != null) {
			
			if(condition instanceof SimpleCondition) {
				EmployeesAttribute attr = ((SimpleCondition<EmployeesAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<EmployeesAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<EmployeesAttribute>) condition).getValue();
				if(value != null) {
					boolean isConditionAttrEncountered = false;
					if(attr == EmployeesAttribute.employeeID ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "EmployeeID " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == EmployeesAttribute.LastName ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "LastName " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == EmployeesAttribute.FirstName ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "FirstName " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == EmployeesAttribute.Title ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "Title " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == EmployeesAttribute.TitleOfCourtesy ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "TitleOfCourtesy " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == EmployeesAttribute.BirthDate ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "BirthDate " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == EmployeesAttribute.HireDate ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "HireDate " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == EmployeesAttribute.Address ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "Address " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == EmployeesAttribute.City ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "City " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == EmployeesAttribute.Region ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "Region " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == EmployeesAttribute.PostalCode ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "PostalCode " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == EmployeesAttribute.Country ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "Country " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == EmployeesAttribute.HomePhone ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "HomePhone " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == EmployeesAttribute.Extension ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "Extension " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == EmployeesAttribute.Photo ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "Photo " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == EmployeesAttribute.Notes ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "Notes " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == EmployeesAttribute.PhotoPath ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "PhotoPath " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == EmployeesAttribute.Salary ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "Salary " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						where = "1 = 1";
					}
				} else {
					if(attr == EmployeesAttribute.employeeID ) {
						if(op == Operator.EQUALS)
							where =  "EmployeeID IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "EmployeeID IS NOT NULL";
					}
					if(attr == EmployeesAttribute.LastName ) {
						if(op == Operator.EQUALS)
							where =  "LastName IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "LastName IS NOT NULL";
					}
					if(attr == EmployeesAttribute.FirstName ) {
						if(op == Operator.EQUALS)
							where =  "FirstName IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "FirstName IS NOT NULL";
					}
					if(attr == EmployeesAttribute.Title ) {
						if(op == Operator.EQUALS)
							where =  "Title IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "Title IS NOT NULL";
					}
					if(attr == EmployeesAttribute.TitleOfCourtesy ) {
						if(op == Operator.EQUALS)
							where =  "TitleOfCourtesy IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "TitleOfCourtesy IS NOT NULL";
					}
					if(attr == EmployeesAttribute.BirthDate ) {
						if(op == Operator.EQUALS)
							where =  "BirthDate IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "BirthDate IS NOT NULL";
					}
					if(attr == EmployeesAttribute.HireDate ) {
						if(op == Operator.EQUALS)
							where =  "HireDate IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "HireDate IS NOT NULL";
					}
					if(attr == EmployeesAttribute.Address ) {
						if(op == Operator.EQUALS)
							where =  "Address IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "Address IS NOT NULL";
					}
					if(attr == EmployeesAttribute.City ) {
						if(op == Operator.EQUALS)
							where =  "City IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "City IS NOT NULL";
					}
					if(attr == EmployeesAttribute.Region ) {
						if(op == Operator.EQUALS)
							where =  "Region IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "Region IS NOT NULL";
					}
					if(attr == EmployeesAttribute.PostalCode ) {
						if(op == Operator.EQUALS)
							where =  "PostalCode IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "PostalCode IS NOT NULL";
					}
					if(attr == EmployeesAttribute.Country ) {
						if(op == Operator.EQUALS)
							where =  "Country IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "Country IS NOT NULL";
					}
					if(attr == EmployeesAttribute.HomePhone ) {
						if(op == Operator.EQUALS)
							where =  "HomePhone IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "HomePhone IS NOT NULL";
					}
					if(attr == EmployeesAttribute.Extension ) {
						if(op == Operator.EQUALS)
							where =  "Extension IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "Extension IS NOT NULL";
					}
					if(attr == EmployeesAttribute.Photo ) {
						if(op == Operator.EQUALS)
							where =  "Photo IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "Photo IS NOT NULL";
					}
					if(attr == EmployeesAttribute.Notes ) {
						if(op == Operator.EQUALS)
							where =  "Notes IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "Notes IS NOT NULL";
					}
					if(attr == EmployeesAttribute.PhotoPath ) {
						if(op == Operator.EQUALS)
							where =  "PhotoPath IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "PhotoPath IS NOT NULL";
					}
					if(attr == EmployeesAttribute.Salary ) {
						if(op == Operator.EQUALS)
							where =  "Salary IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "Salary IS NOT NULL";
					}
				}
			}
	
			if(condition instanceof AndCondition) {
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInEmployeesFromMyRelDB(((AndCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInEmployeesFromMyRelDB(((AndCondition) condition).getRightCondition(), refilterFlag);
				String whereLeft = pairLeft.getKey();
				String whereRight = pairRight.getKey();
				List<String> leftValues = pairLeft.getValue();
				List<String> rightValues = pairRight.getValue();
				if(whereLeft != null || whereRight != null) {
					if(whereLeft == null)
						where = whereRight;
					else
						if(whereRight == null)
							where = whereLeft;
						else
							where = "(" + whereLeft + " AND " + whereRight + ")";
					preparedValues.addAll(leftValues);
					preparedValues.addAll(rightValues);
				}
			}
	
			if(condition instanceof OrCondition) {
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInEmployeesFromMyRelDB(((OrCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInEmployeesFromMyRelDB(((OrCondition) condition).getRightCondition(), refilterFlag);
				String whereLeft = pairLeft.getKey();
				String whereRight = pairRight.getKey();
				List<String> leftValues = pairLeft.getValue();
				List<String> rightValues = pairRight.getValue();
				if(whereLeft != null || whereRight != null) {
					if(whereLeft == null)
						where = whereRight;
					else
						if(whereRight == null)
							where = whereLeft;
						else
							where = "(" + whereLeft + " OR " + whereRight + ")";
					preparedValues.addAll(leftValues);
					preparedValues.addAll(rightValues);
				}
			}
	
		}
	
		return new ImmutablePair<String, List<String>>(where, preparedValues);
	}
	
	
	
	public Dataset<Employees> getEmployeesListInEmployeesFromMyRelDB(conditions.Condition<conditions.EmployeesAttribute> condition, MutableBoolean refilterFlag){
	
		Pair<String, List<String>> whereClause = EmployeesServiceImpl.getSQLWhereClauseInEmployeesFromMyRelDB(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", preparedValue);
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("myRelDB", "Employees", where);
		
	
		Dataset<Employees> res = d.map((MapFunction<Row, Employees>) r -> {
					Employees employees_res = new Employees();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
					// attribute [Employees.EmployeeID]
					Integer employeeID = Util.getIntegerValue(r.getAs("EmployeeID"));
					employees_res.setEmployeeID(employeeID);
					
					// attribute [Employees.LastName]
					String lastName = Util.getStringValue(r.getAs("LastName"));
					employees_res.setLastName(lastName);
					
					// attribute [Employees.FirstName]
					String firstName = Util.getStringValue(r.getAs("FirstName"));
					employees_res.setFirstName(firstName);
					
					// attribute [Employees.Title]
					String title = Util.getStringValue(r.getAs("Title"));
					employees_res.setTitle(title);
					
					// attribute [Employees.TitleOfCourtesy]
					String titleOfCourtesy = Util.getStringValue(r.getAs("TitleOfCourtesy"));
					employees_res.setTitleOfCourtesy(titleOfCourtesy);
					
					// attribute [Employees.BirthDate]
					LocalDate birthDate = Util.getLocalDateValue(r.getAs("BirthDate"));
					employees_res.setBirthDate(birthDate);
					
					// attribute [Employees.HireDate]
					LocalDate hireDate = Util.getLocalDateValue(r.getAs("HireDate"));
					employees_res.setHireDate(hireDate);
					
					// attribute [Employees.Address]
					String address = Util.getStringValue(r.getAs("Address"));
					employees_res.setAddress(address);
					
					// attribute [Employees.City]
					String city = Util.getStringValue(r.getAs("City"));
					employees_res.setCity(city);
					
					// attribute [Employees.Region]
					String region = Util.getStringValue(r.getAs("Region"));
					employees_res.setRegion(region);
					
					// attribute [Employees.PostalCode]
					String postalCode = Util.getStringValue(r.getAs("PostalCode"));
					employees_res.setPostalCode(postalCode);
					
					// attribute [Employees.Country]
					String country = Util.getStringValue(r.getAs("Country"));
					employees_res.setCountry(country);
					
					// attribute [Employees.HomePhone]
					String homePhone = Util.getStringValue(r.getAs("HomePhone"));
					employees_res.setHomePhone(homePhone);
					
					// attribute [Employees.Extension]
					String extension = Util.getStringValue(r.getAs("Extension"));
					employees_res.setExtension(extension);
					
					// attribute [Employees.Photo]
					byte[] photo = Util.getByteArrayValue(r.getAs("Photo"));
					employees_res.setPhoto(photo);
					
					// attribute [Employees.Notes]
					String notes = Util.getStringValue(r.getAs("Notes"));
					employees_res.setNotes(notes);
					
					// attribute [Employees.PhotoPath]
					String photoPath = Util.getStringValue(r.getAs("PhotoPath"));
					employees_res.setPhotoPath(photoPath);
					
					// attribute [Employees.Salary]
					Double salary = Util.getDoubleValue(r.getAs("Salary"));
					employees_res.setSalary(salary);
	
	
	
					return employees_res;
				}, Encoders.bean(Employees.class));
	
	
		return res;
		
	}
	
	
	
	
	
	
	public Dataset<Employees> getEmployedListInWorks(conditions.Condition<conditions.EmployeesAttribute> employed_condition,conditions.Condition<conditions.TerritoriesAttribute> territories_condition)		{
		MutableBoolean employed_refilter = new MutableBoolean(false);
		List<Dataset<Employees>> datasetsPOJO = new ArrayList<Dataset<Employees>>();
		Dataset<Territories> all = null;
		boolean all_already_persisted = false;
		MutableBoolean territories_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		// join physical structure A<-AB->B
		
		//join between 3 SQL tables stored in same db
		// (A AB B)
		territories_refilter = new MutableBoolean(false);
		MutableBoolean works_refilter = new MutableBoolean(false);
		Dataset<Works> res_Employees_employee_territory = worksService.getWorksListInEmployeesAndEmployeeTerritoriesAndTerritoriesFrommyRelDB(employed_condition, territories_condition, employed_refilter, territories_refilter);
		Dataset<Employees> res_Employees_employee = null;
		
		if(territories_refilter.booleanValue()) {
			if(all == null)
					all = new TerritoriesServiceImpl().getTerritoriesList(territories_condition);
			joinCondition = null;
				joinCondition = res_Employees_employee_territory.col("territories.territoryID").equalTo(all.col("territoryID"));
				if(joinCondition == null)
					res_Employees_employee = res_Employees_employee_territory.join(all).select("employed.*").as(Encoders.bean(Employees.class));
				else
					res_Employees_employee = res_Employees_employee_territory.join(all, joinCondition).select("employed.*").as(Encoders.bean(Employees.class));
		} else
			res_Employees_employee = res_Employees_employee_territory.map((MapFunction<Works,Employees>) r -> r.getEmployed(), Encoders.bean(Employees.class));
			
		datasetsPOJO.add(res_Employees_employee.dropDuplicates(new String[] {"employeeID"}));	
		
		
		
		Dataset<Works> res_works_employed;
		Dataset<Employees> res_Employees;
		
		
		//Join datasets or return 
		Dataset<Employees> res = fullOuterJoinsEmployees(datasetsPOJO);
		if(res == null)
			return null;
	
		if(employed_refilter.booleanValue())
			res = res.filter((FilterFunction<Employees>) r -> employed_condition == null || employed_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Employees> getSubordoneeListInReportsTo(conditions.Condition<conditions.EmployeesAttribute> subordonee_condition,conditions.Condition<conditions.EmployeesAttribute> boss_condition)		{
		MutableBoolean subordonee_refilter = new MutableBoolean(false);
		List<Dataset<Employees>> datasetsPOJO = new ArrayList<Dataset<Employees>>();
		Dataset<Employees> all = null;
		boolean all_already_persisted = false;
		MutableBoolean boss_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		// For role 'subordonee' in reference 'manager'. A->B Scenario in SQL DB
		boss_refilter = new MutableBoolean(false);
		Dataset<ReportsTo> res_manager = reportsToService.getSubordoneeAndBossListInManagerInEmployeesFromMyRelDB(subordonee_condition, boss_condition, subordonee_refilter, boss_refilter);
		Dataset<Employees> res_Employees_manager = null;
		if(boss_refilter.booleanValue()) {
			if(all == null)
				all = new EmployeesServiceImpl().getEmployeesList(boss_condition);
			joinCondition = null;
			joinCondition = res_manager.col("boss.employeeID").equalTo(all.col("employeeID"));
			if(joinCondition == null)
				res_Employees_manager = res_manager.join(all).select("subordonee.*").as(Encoders.bean(Employees.class));
			else
				res_Employees_manager = res_manager.join(all, joinCondition).select("subordonee.*").as(Encoders.bean(Employees.class));
		} else
			res_Employees_manager = res_manager.map((MapFunction<ReportsTo,Employees>) r -> r.getSubordonee(), Encoders.bean(Employees.class));
		res_Employees_manager = res_Employees_manager.dropDuplicates(new String[] {"employeeID"});
		datasetsPOJO.add(res_Employees_manager);
		
		
		Dataset<ReportsTo> res_reportsTo_subordonee;
		Dataset<Employees> res_Employees;
		
		
		//Join datasets or return 
		Dataset<Employees> res = fullOuterJoinsEmployees(datasetsPOJO);
		if(res == null)
			return null;
	
		if(subordonee_refilter.booleanValue())
			res = res.filter((FilterFunction<Employees>) r -> subordonee_condition == null || subordonee_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Employees> getBossListInReportsTo(conditions.Condition<conditions.EmployeesAttribute> subordonee_condition,conditions.Condition<conditions.EmployeesAttribute> boss_condition)		{
		MutableBoolean boss_refilter = new MutableBoolean(false);
		List<Dataset<Employees>> datasetsPOJO = new ArrayList<Dataset<Employees>>();
		Dataset<Employees> all = null;
		boolean all_already_persisted = false;
		MutableBoolean subordonee_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		subordonee_refilter = new MutableBoolean(false);
		// For role 'subordonee' in reference 'manager'  B->A Scenario in SQL Db.
		Dataset<ReportsTo> res_manager = reportsToService.getSubordoneeAndBossListInManagerInEmployeesFromMyRelDB(subordonee_condition, boss_condition, subordonee_refilter, boss_refilter);
		Dataset<Employees> res_Employees_manager = null;
		if(subordonee_refilter.booleanValue()) {
			if(all == null)
				all = new EmployeesServiceImpl().getEmployeesList(subordonee_condition);
			joinCondition = null;
			joinCondition = res_manager.col("subordonee.employeeID").equalTo(all.col("employeeID"));
			if(joinCondition == null)
				res_Employees_manager = res_manager.join(all).select("boss.*").as(Encoders.bean(Employees.class));
			else
				res_Employees_manager = res_manager.join(all, joinCondition).select("boss.*").as(Encoders.bean(Employees.class));
		} else
			res_Employees_manager = res_manager.map((MapFunction<ReportsTo,Employees>) r -> r.getBoss(), Encoders.bean(Employees.class));
		res_Employees_manager = res_Employees_manager.dropDuplicates(new String[] {"employeeID"});
		datasetsPOJO.add(res_Employees_manager);
		
		Dataset<ReportsTo> res_reportsTo_boss;
		Dataset<Employees> res_Employees;
		
		
		//Join datasets or return 
		Dataset<Employees> res = fullOuterJoinsEmployees(datasetsPOJO);
		if(res == null)
			return null;
	
		if(boss_refilter.booleanValue())
			res = res.filter((FilterFunction<Employees>) r -> boss_condition == null || boss_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Employees> getEmployeeInChargeListInRegister(conditions.Condition<conditions.OrdersAttribute> processedOrder_condition,conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition)		{
		MutableBoolean employeeInCharge_refilter = new MutableBoolean(false);
		List<Dataset<Employees>> datasetsPOJO = new ArrayList<Dataset<Employees>>();
		Dataset<Orders> all = null;
		boolean all_already_persisted = false;
		MutableBoolean processedOrder_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		processedOrder_refilter = new MutableBoolean(false);
		// For role 'processedOrder' in reference 'encoded'  B->A Scenario
		Dataset<OrdersTDO> ordersTDOencodedprocessedOrder = registerService.getOrdersTDOListProcessedOrderInEncodedInOrdersFromMongoDB(processedOrder_condition, processedOrder_refilter);
		Dataset<EmployeesTDO> employeesTDOencodedemployeeInCharge = registerService.getEmployeesTDOListEmployeeInChargeInEncodedInOrdersFromMongoDB(employeeInCharge_condition, employeeInCharge_refilter);
		if(processedOrder_refilter.booleanValue()) {
			if(all == null)
				all = new OrdersServiceImpl().getOrdersList(processedOrder_condition);
			joinCondition = null;
			joinCondition = ordersTDOencodedprocessedOrder.col("id").equalTo(all.col("id"));
			if(joinCondition == null)
				ordersTDOencodedprocessedOrder = ordersTDOencodedprocessedOrder.as("A").join(all).select("A.*").as(Encoders.bean(OrdersTDO.class));
			else
				ordersTDOencodedprocessedOrder = ordersTDOencodedprocessedOrder.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(OrdersTDO.class));
		}
		Dataset<Row> res_encoded = 
			employeesTDOencodedemployeeInCharge.join(ordersTDOencodedprocessedOrder
				.withColumnRenamed("id", "Orders_id")
				.withColumnRenamed("orderDate", "Orders_orderDate")
				.withColumnRenamed("requiredDate", "Orders_requiredDate")
				.withColumnRenamed("shippedDate", "Orders_shippedDate")
				.withColumnRenamed("freight", "Orders_freight")
				.withColumnRenamed("shipName", "Orders_shipName")
				.withColumnRenamed("shipAddress", "Orders_shipAddress")
				.withColumnRenamed("shipCity", "Orders_shipCity")
				.withColumnRenamed("shipRegion", "Orders_shipRegion")
				.withColumnRenamed("shipPostalCode", "Orders_shipPostalCode")
				.withColumnRenamed("shipCountry", "Orders_shipCountry")
				.withColumnRenamed("logEvents", "Orders_logEvents"),
				employeesTDOencodedemployeeInCharge.col("mongoDB_Orders_encoded_EmployeeID").equalTo(ordersTDOencodedprocessedOrder.col("mongoDB_Orders_encoded_EmployeeRef")));
		Dataset<Employees> res_Employees_encoded = res_encoded.select( "employeeID", "LastName", "FirstName", "Title", "TitleOfCourtesy", "BirthDate", "HireDate", "Address", "City", "Region", "PostalCode", "Country", "HomePhone", "Extension", "Photo", "Notes", "PhotoPath", "Salary", "logEvents").as(Encoders.bean(Employees.class));
		res_Employees_encoded = res_Employees_encoded.dropDuplicates(new String[] {"employeeID"});
		datasetsPOJO.add(res_Employees_encoded);
		
		Dataset<Register> res_register_employeeInCharge;
		Dataset<Employees> res_Employees;
		
		
		//Join datasets or return 
		Dataset<Employees> res = fullOuterJoinsEmployees(datasetsPOJO);
		if(res == null)
			return null;
	
		if(employeeInCharge_refilter.booleanValue())
			res = res.filter((FilterFunction<Employees>) r -> employeeInCharge_condition == null || employeeInCharge_condition.evaluate(r));
		
	
		return res;
		}
	
	
	public boolean insertEmployees(Employees employees){
		// Insert into all mapped standalone AbstractPhysicalStructure 
		boolean inserted = false;
			inserted = insertEmployeesInEmployeesFromMyRelDB(employees) || inserted ;
		return inserted;
	}
	
	public boolean insertEmployeesInEmployeesFromMyRelDB(Employees employees)	{
		String idvalue="";
		idvalue+=employees.getEmployeeID();
		boolean entityExists = false; // Modify in acceleo code (in 'main.services.insert.entitytype.generateSimpleInsertMethods.mtl') to generate checking before insert
		if(!entityExists){
		List<String> columns = new ArrayList<>();
		List<Object> values = new ArrayList<>();	
		columns.add("EmployeeID");
		values.add(employees.getEmployeeID());
		columns.add("LastName");
		values.add(employees.getLastName());
		columns.add("FirstName");
		values.add(employees.getFirstName());
		columns.add("Title");
		values.add(employees.getTitle());
		columns.add("TitleOfCourtesy");
		values.add(employees.getTitleOfCourtesy());
		columns.add("BirthDate");
		values.add(employees.getBirthDate());
		columns.add("HireDate");
		values.add(employees.getHireDate());
		columns.add("HomePhone");
		values.add(employees.getHomePhone());
		columns.add("Extension");
		values.add(employees.getExtension());
		columns.add("Photo");
		values.add(employees.getPhoto());
		columns.add("Notes");
		values.add(employees.getNotes());
		columns.add("PhotoPath");
		values.add(employees.getPhotoPath());
		columns.add("Salary");
		values.add(employees.getSalary());
		columns.add("Address");
		values.add(employees.getAddress());
		columns.add("City");
		values.add(employees.getCity());
		columns.add("Region");
		values.add(employees.getRegion());
		columns.add("PostalCode");
		values.add(employees.getPostalCode());
		columns.add("Country");
		values.add(employees.getCountry());
		DBConnectionMgr.insertInTable(columns, Arrays.asList(values), "Employees", "myRelDB");
			logger.info("Inserted [Employees] entity ID [{}] in [Employees] in database [MyRelDB]", idvalue);
		}
		else
			logger.warn("[Employees] entity ID [{}] already present in [Employees] in database [MyRelDB]", idvalue);
		return !entityExists;
	} 
	
	private boolean inUpdateMethod = false;
	private List<Row> allEmployeesIdList = null;
	public void updateEmployeesList(conditions.Condition<conditions.EmployeesAttribute> condition, conditions.SetClause<conditions.EmployeesAttribute> set){
		inUpdateMethod = true;
		try {
			MutableBoolean refilterInEmployeesFromMyRelDB = new MutableBoolean(false);
			getSQLWhereClauseInEmployeesFromMyRelDB(condition, refilterInEmployeesFromMyRelDB);
			// one first updates in the structures necessitating to execute a "SELECT *" query to establish the update condition 
			if(refilterInEmployeesFromMyRelDB.booleanValue())
				updateEmployeesListInEmployeesFromMyRelDB(condition, set);
		
	
			if(!refilterInEmployeesFromMyRelDB.booleanValue())
				updateEmployeesListInEmployeesFromMyRelDB(condition, set);
	
		} finally {
			inUpdateMethod = false;
		}
	}
	
	
	public void updateEmployeesListInEmployeesFromMyRelDB(Condition<EmployeesAttribute> condition, SetClause<EmployeesAttribute> set) {
		List<String> setClause = EmployeesServiceImpl.getSQLSetClauseInEmployeesFromMyRelDB(set);
		String setSQL = null;
		for(int i = 0; i < setClause.size(); i++) {
			if(i == 0)
				setSQL = setClause.get(i);
			else
				setSQL += ", " + setClause.get(i);
		}
		
		if(setSQL == null)
			return;
		
		MutableBoolean refilter = new MutableBoolean(false);
		Pair<String, List<String>> whereClause = EmployeesServiceImpl.getSQLWhereClauseInEmployeesFromMyRelDB(condition, refilter);
		if(!refilter.booleanValue()) {
			String where = whereClause.getKey();
			List<String> preparedValues = whereClause.getValue();
			for(String preparedValue : preparedValues) {
				where = where.replaceFirst("\\?", preparedValue);
			}
			
			String sql = "UPDATE Employees SET " + setSQL;
			if(where != null)
				sql += " WHERE " + where;
			
			DBConnectionMgr.updateInTable(sql, "myRelDB");
		} else {
			if(!inUpdateMethod || allEmployeesIdList == null)
				allEmployeesIdList = this.getEmployeesList(condition).select("employeeID").collectAsList();
		
			List<String> updateQueries = new ArrayList<String>();
			for(Row row : allEmployeesIdList) {
				Condition<EmployeesAttribute> conditionId = null;
				conditionId = Condition.simple(EmployeesAttribute.employeeID, Operator.EQUALS, row.getAs("employeeID"));
				whereClause = EmployeesServiceImpl.getSQLWhereClauseInEmployeesFromMyRelDB(conditionId, refilter);
				String sql = "UPDATE Employees SET " + setSQL;
				String where = whereClause.getKey();
				List<String> preparedValues = whereClause.getValue();
				for(String preparedValue : preparedValues) {
					where = where.replaceFirst("\\?", preparedValue);
				}
				if(where != null)
					sql += " WHERE " + where;
				updateQueries.add(sql);
			}
		
			DBConnectionMgr.updatesInTable(updateQueries, "myRelDB");
		}
		
	}
	
	
	
	public void updateEmployees(pojo.Employees employees) {
		//TODO using the id
		return;
	}
	public void updateEmployedListInWorks(
		conditions.Condition<conditions.EmployeesAttribute> employed_condition,
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition,
		
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		//TODO
	}
	
	public void updateEmployedListInWorksByEmployedCondition(
		conditions.Condition<conditions.EmployeesAttribute> employed_condition,
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		updateEmployedListInWorks(employed_condition, null, set);
	}
	public void updateEmployedListInWorksByTerritoriesCondition(
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition,
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		updateEmployedListInWorks(null, territories_condition, set);
	}
	
	public void updateEmployedListInWorksByTerritories(
		pojo.Territories territories,
		conditions.SetClause<conditions.EmployeesAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateSubordoneeListInReportsTo(
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition,
		conditions.Condition<conditions.EmployeesAttribute> boss_condition,
		
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		//TODO
	}
	
	public void updateSubordoneeListInReportsToBySubordoneeCondition(
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition,
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		updateSubordoneeListInReportsTo(subordonee_condition, null, set);
	}
	public void updateSubordoneeListInReportsToByBossCondition(
		conditions.Condition<conditions.EmployeesAttribute> boss_condition,
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		updateSubordoneeListInReportsTo(null, boss_condition, set);
	}
	
	public void updateSubordoneeListInReportsToByBoss(
		pojo.Employees boss,
		conditions.SetClause<conditions.EmployeesAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateBossListInReportsTo(
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition,
		conditions.Condition<conditions.EmployeesAttribute> boss_condition,
		
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		//TODO
	}
	
	public void updateBossListInReportsToBySubordoneeCondition(
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition,
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		updateBossListInReportsTo(subordonee_condition, null, set);
	}
	
	public void updateBossInReportsToBySubordonee(
		pojo.Employees subordonee,
		conditions.SetClause<conditions.EmployeesAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateBossListInReportsToByBossCondition(
		conditions.Condition<conditions.EmployeesAttribute> boss_condition,
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		updateBossListInReportsTo(null, boss_condition, set);
	}
	public void updateEmployeeInChargeListInRegister(
		conditions.Condition<conditions.OrdersAttribute> processedOrder_condition,
		conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition,
		
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		//TODO
	}
	
	public void updateEmployeeInChargeListInRegisterByProcessedOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> processedOrder_condition,
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		updateEmployeeInChargeListInRegister(processedOrder_condition, null, set);
	}
	
	public void updateEmployeeInChargeInRegisterByProcessedOrder(
		pojo.Orders processedOrder,
		conditions.SetClause<conditions.EmployeesAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateEmployeeInChargeListInRegisterByEmployeeInChargeCondition(
		conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition,
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		updateEmployeeInChargeListInRegister(null, employeeInCharge_condition, set);
	}
	
	
	public void deleteEmployeesList(conditions.Condition<conditions.EmployeesAttribute> condition){
		//TODO
	}
	
	public void deleteEmployees(pojo.Employees employees) {
		//TODO using the id
		return;
	}
	public void deleteEmployedListInWorks(	
		conditions.Condition<conditions.EmployeesAttribute> employed_condition,	
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition){
			//TODO
		}
	
	public void deleteEmployedListInWorksByEmployedCondition(
		conditions.Condition<conditions.EmployeesAttribute> employed_condition
	){
		deleteEmployedListInWorks(employed_condition, null);
	}
	public void deleteEmployedListInWorksByTerritoriesCondition(
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition
	){
		deleteEmployedListInWorks(null, territories_condition);
	}
	
	public void deleteEmployedListInWorksByTerritories(
		pojo.Territories territories 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteSubordoneeListInReportsTo(	
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition,	
		conditions.Condition<conditions.EmployeesAttribute> boss_condition){
			//TODO
		}
	
	public void deleteSubordoneeListInReportsToBySubordoneeCondition(
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition
	){
		deleteSubordoneeListInReportsTo(subordonee_condition, null);
	}
	public void deleteSubordoneeListInReportsToByBossCondition(
		conditions.Condition<conditions.EmployeesAttribute> boss_condition
	){
		deleteSubordoneeListInReportsTo(null, boss_condition);
	}
	
	public void deleteSubordoneeListInReportsToByBoss(
		pojo.Employees boss 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteBossListInReportsTo(	
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition,	
		conditions.Condition<conditions.EmployeesAttribute> boss_condition){
			//TODO
		}
	
	public void deleteBossListInReportsToBySubordoneeCondition(
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition
	){
		deleteBossListInReportsTo(subordonee_condition, null);
	}
	
	public void deleteBossInReportsToBySubordonee(
		pojo.Employees subordonee 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteBossListInReportsToByBossCondition(
		conditions.Condition<conditions.EmployeesAttribute> boss_condition
	){
		deleteBossListInReportsTo(null, boss_condition);
	}
	public void deleteEmployeeInChargeListInRegister(	
		conditions.Condition<conditions.OrdersAttribute> processedOrder_condition,	
		conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition){
			//TODO
		}
	
	public void deleteEmployeeInChargeListInRegisterByProcessedOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> processedOrder_condition
	){
		deleteEmployeeInChargeListInRegister(processedOrder_condition, null);
	}
	
	public void deleteEmployeeInChargeInRegisterByProcessedOrder(
		pojo.Orders processedOrder 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteEmployeeInChargeListInRegisterByEmployeeInChargeCondition(
		conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition
	){
		deleteEmployeeInChargeListInRegister(null, employeeInCharge_condition);
	}
	
}
