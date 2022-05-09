package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Orders;
import conditions.*;
import dao.services.OrdersService;
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


public class OrdersServiceImpl extends OrdersService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OrdersServiceImpl.class);
	
	
	
	
	
	
	public static Pair<List<String>, List<String>> getBSONUpdateQueryInOrdersFromMyMongoDB(conditions.SetClause<OrdersAttribute> set) {
		List<String> res = new ArrayList<String>();
		Set<String> arrayFields = new HashSet<String>();
		if(set != null) {
			java.util.Map<String, java.util.Map<String, String>> longFieldValues = new java.util.HashMap<String, java.util.Map<String, String>>();
			java.util.Map<OrdersAttribute, Object> clause = set.getClause();
			for(java.util.Map.Entry<OrdersAttribute, Object> e : clause.entrySet()) {
				OrdersAttribute attr = e.getKey();
				Object value = e.getValue();
				if(attr == OrdersAttribute.id ) {
					String fieldName = "OrderID";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrdersAttribute.orderDate ) {
					String fieldName = "OrderDate";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrdersAttribute.requiredDate ) {
					String fieldName = "RequiredDate";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrdersAttribute.shippedDate ) {
					String fieldName = "ShippedDate";
					fieldName = "ShipmentInfo." + fieldName;
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrdersAttribute.freight ) {
					String fieldName = "Freight";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrdersAttribute.shipName ) {
					String fieldName = "ShipName";
					fieldName = "ShipmentInfo." + fieldName;
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrdersAttribute.shipAddress ) {
					String fieldName = "ShipAddress";
					fieldName = "ShipmentInfo." + fieldName;
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrdersAttribute.shipCity ) {
					String fieldName = "ShipCity";
					fieldName = "ShipmentInfo." + fieldName;
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrdersAttribute.shipRegion ) {
					String fieldName = "ShipRegion";
					fieldName = "ShipmentInfo." + fieldName;
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrdersAttribute.shipPostalCode ) {
					String fieldName = "ShipPostalCode";
					fieldName = "ShipmentInfo." + fieldName;
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrdersAttribute.shipCountry ) {
					String fieldName = "ShipCountry";
					fieldName = "ShipmentInfo." + fieldName;
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
			}
	
			for(java.util.Map.Entry<String, java.util.Map<String, String>> entry : longFieldValues.entrySet()) {
				String longField = entry.getKey();
				java.util.Map<String, String> values = entry.getValue();
			}
	
		}
		return new ImmutablePair<List<String>, List<String>>(res, new ArrayList<String>(arrayFields));
	}
	
	public static String getBSONMatchQueryInOrdersFromMyMongoDB(Condition<OrdersAttribute> condition, MutableBoolean refilterFlag) {	
		String res = null;	
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				OrdersAttribute attr = ((SimpleCondition<OrdersAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<OrdersAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<OrdersAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == OrdersAttribute.id ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "OrderID': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == OrdersAttribute.orderDate ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "OrderDate': {" + mongoOp + ": " +  "ISODate("+preparedValue + ")}";
	
					res = "'" + res;
					}
					if(attr == OrdersAttribute.requiredDate ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "RequiredDate': {" + mongoOp + ": " +  "ISODate("+preparedValue + ")}";
	
					res = "'" + res;
					}
					if(attr == OrdersAttribute.shippedDate ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "ShippedDate': {" + mongoOp + ": " +  "ISODate("+preparedValue + ")}";
	
						res = "ShipmentInfo." + res;
					res = "'" + res;
					}
					if(attr == OrdersAttribute.freight ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "Freight': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == OrdersAttribute.shipName ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "ShipName': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "ShipmentInfo." + res;
					res = "'" + res;
					}
					if(attr == OrdersAttribute.shipAddress ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "ShipAddress': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "ShipmentInfo." + res;
					res = "'" + res;
					}
					if(attr == OrdersAttribute.shipCity ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "ShipCity': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "ShipmentInfo." + res;
					res = "'" + res;
					}
					if(attr == OrdersAttribute.shipRegion ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "ShipRegion': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "ShipmentInfo." + res;
					res = "'" + res;
					}
					if(attr == OrdersAttribute.shipPostalCode ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "ShipPostalCode': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "ShipmentInfo." + res;
					res = "'" + res;
					}
					if(attr == OrdersAttribute.shipCountry ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "ShipCountry': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "ShipmentInfo." + res;
					res = "'" + res;
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						res = "$expr: {$eq:[1,1]}";
					}
					
				}
			}
	
			if(condition instanceof AndCondition) {
				String bsonLeft = getBSONMatchQueryInOrdersFromMyMongoDB(((AndCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInOrdersFromMyMongoDB(((AndCondition)condition).getRightCondition(), refilterFlag);			
				if(bsonLeft == null && bsonRight == null)
					return null;
				if(bsonLeft == null)
					return bsonRight;
				if(bsonRight == null)
					return bsonLeft;
				res = " $and: [ {" + bsonLeft + "}, {" + bsonRight + "}] ";
			}
	
			if(condition instanceof OrCondition) {
				String bsonLeft = getBSONMatchQueryInOrdersFromMyMongoDB(((OrCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInOrdersFromMyMongoDB(((OrCondition)condition).getRightCondition(), refilterFlag);			
				if(bsonLeft == null && bsonRight == null)
					return null;
				if(bsonLeft == null)
					return bsonRight;
				if(bsonRight == null)
					return bsonLeft;
				res = " $or: [ {" + bsonLeft + "}, {" + bsonRight + "}] ";	
			}
	
			
	
			
		}
	
		return res;
	}
	
	public static Pair<String, List<String>> getBSONQueryAndArrayFilterForUpdateQueryInOrdersFromMyMongoDB(Condition<OrdersAttribute> condition, final List<String> arrayVariableNames, Set<String> arrayVariablesUsed, MutableBoolean refilterFlag) {	
		String query = null;
		List<String> arrayFilters = new ArrayList<String>();
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				String bson = null;
				OrdersAttribute attr = ((SimpleCondition<OrdersAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<OrdersAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<OrdersAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == OrdersAttribute.id ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "OrderID': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrdersAttribute.orderDate ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "OrderDate': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrdersAttribute.requiredDate ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "RequiredDate': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrdersAttribute.shippedDate ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "ShippedDate': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
						if(!arrayVar) {
							bson = "ShipmentInfo." + bson;
						}
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrdersAttribute.freight ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "Freight': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrdersAttribute.shipName ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "ShipName': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
						if(!arrayVar) {
							bson = "ShipmentInfo." + bson;
						}
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrdersAttribute.shipAddress ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "ShipAddress': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
						if(!arrayVar) {
							bson = "ShipmentInfo." + bson;
						}
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrdersAttribute.shipCity ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "ShipCity': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
						if(!arrayVar) {
							bson = "ShipmentInfo." + bson;
						}
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrdersAttribute.shipRegion ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "ShipRegion': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
						if(!arrayVar) {
							bson = "ShipmentInfo." + bson;
						}
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrdersAttribute.shipPostalCode ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "ShipPostalCode': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
						if(!arrayVar) {
							bson = "ShipmentInfo." + bson;
						}
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrdersAttribute.shipCountry ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "ShipCountry': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
						if(!arrayVar) {
							bson = "ShipmentInfo." + bson;
						}
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
					}
					
				}
	
			}
	
			if(condition instanceof AndCondition) {
				Pair<String, List<String>> bsonLeft = getBSONQueryAndArrayFilterForUpdateQueryInOrdersFromMyMongoDB(((AndCondition)condition).getLeftCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);
				Pair<String, List<String>> bsonRight = getBSONQueryAndArrayFilterForUpdateQueryInOrdersFromMyMongoDB(((AndCondition)condition).getRightCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);			
				
				String queryLeft = bsonLeft.getLeft();
				String queryRight = bsonRight.getLeft();
				List<String> arrayFilterLeft = bsonLeft.getRight();
				List<String> arrayFilterRight = bsonRight.getRight();
	
				if(queryLeft == null && queryRight != null)
					query = queryRight;
				if(queryLeft != null && queryRight == null)
					query = queryLeft;
				if(queryLeft != null && queryRight != null)
					query = " $and: [ {" + queryLeft + "}, {" + queryRight + "}] ";
	
				arrayFilters.addAll(arrayFilterLeft);
				arrayFilters.addAll(arrayFilterRight);
			}
	
			if(condition instanceof OrCondition) {
				Pair<String, List<String>> bsonLeft = getBSONQueryAndArrayFilterForUpdateQueryInOrdersFromMyMongoDB(((AndCondition)condition).getLeftCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);
				Pair<String, List<String>> bsonRight = getBSONQueryAndArrayFilterForUpdateQueryInOrdersFromMyMongoDB(((AndCondition)condition).getRightCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);			
				
				String queryLeft = bsonLeft.getLeft();
				String queryRight = bsonRight.getLeft();
				List<String> arrayFilterLeft = bsonLeft.getRight();
				List<String> arrayFilterRight = bsonRight.getRight();
	
				if(queryLeft == null && queryRight != null)
					query = queryRight;
				if(queryLeft != null && queryRight == null)
					query = queryLeft;
				if(queryLeft != null && queryRight != null)
					query = " $or: [ {" + queryLeft + "}, {" + queryRight + "}] ";
	
				arrayFilters.addAll(arrayFilterLeft);
				arrayFilters.addAll(arrayFilterRight); // can be a problem
			}
		}
	
		return new ImmutablePair<String, List<String>>(query, arrayFilters);
	}
	
	
	
	public Dataset<Orders> getOrdersListInOrdersFromMyMongoDB(conditions.Condition<conditions.OrdersAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = OrdersServiceImpl.getBSONMatchQueryInOrdersFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Orders", bsonQuery);
	
		Dataset<Orders> res = dataset.flatMap((FlatMapFunction<Row, Orders>) r -> {
				Set<Orders> list_res = new HashSet<Orders>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				Orders orders1 = new Orders();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Orders.id for field OrderID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderID")) {
						if(nestedRow.getAs("OrderID")==null)
							orders1.setId(null);
						else{
							orders1.setId(Util.getIntegerValue(nestedRow.getAs("OrderID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.orderDate for field OrderDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderDate")) {
						if(nestedRow.getAs("OrderDate")==null)
							orders1.setOrderDate(null);
						else{
							orders1.setOrderDate(Util.getLocalDateValue(nestedRow.getAs("OrderDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.requiredDate for field RequiredDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("RequiredDate")) {
						if(nestedRow.getAs("RequiredDate")==null)
							orders1.setRequiredDate(null);
						else{
							orders1.setRequiredDate(Util.getLocalDateValue(nestedRow.getAs("RequiredDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.freight for field Freight			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Freight")) {
						if(nestedRow.getAs("Freight")==null)
							orders1.setFreight(null);
						else{
							orders1.setFreight(Util.getDoubleValue(nestedRow.getAs("Freight")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shippedDate for field ShippedDate			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShippedDate")) {
						if(nestedRow.getAs("ShippedDate")==null)
							orders1.setShippedDate(null);
						else{
							orders1.setShippedDate(Util.getLocalDateValue(nestedRow.getAs("ShippedDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipName for field ShipName			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipName")) {
						if(nestedRow.getAs("ShipName")==null)
							orders1.setShipName(null);
						else{
							orders1.setShipName(Util.getStringValue(nestedRow.getAs("ShipName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipAddress for field ShipAddress			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipAddress")) {
						if(nestedRow.getAs("ShipAddress")==null)
							orders1.setShipAddress(null);
						else{
							orders1.setShipAddress(Util.getStringValue(nestedRow.getAs("ShipAddress")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipCity for field ShipCity			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCity")) {
						if(nestedRow.getAs("ShipCity")==null)
							orders1.setShipCity(null);
						else{
							orders1.setShipCity(Util.getStringValue(nestedRow.getAs("ShipCity")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipRegion for field ShipRegion			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipRegion")) {
						if(nestedRow.getAs("ShipRegion")==null)
							orders1.setShipRegion(null);
						else{
							orders1.setShipRegion(Util.getStringValue(nestedRow.getAs("ShipRegion")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipPostalCode for field ShipPostalCode			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipPostalCode")) {
						if(nestedRow.getAs("ShipPostalCode")==null)
							orders1.setShipPostalCode(null);
						else{
							orders1.setShipPostalCode(Util.getStringValue(nestedRow.getAs("ShipPostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipCountry for field ShipCountry			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCountry")) {
						if(nestedRow.getAs("ShipCountry")==null)
							orders1.setShipCountry(null);
						else{
							orders1.setShipCountry(Util.getStringValue(nestedRow.getAs("ShipCountry")));
							toAdd1 = true;					
							}
					}
					if(toAdd1) {
						list_res.add(orders1);
						addedInList = true;
					} 
					
				
				return list_res.iterator();
	
		}, Encoders.bean(Orders.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
		
	}
	
	
	
	
	
	
	public Dataset<Orders> getBoughtOrderListInBuy(conditions.Condition<conditions.OrdersAttribute> boughtOrder_condition,conditions.Condition<conditions.CustomersAttribute> customer_condition)		{
		MutableBoolean boughtOrder_refilter = new MutableBoolean(false);
		List<Dataset<Orders>> datasetsPOJO = new ArrayList<Dataset<Orders>>();
		Dataset<Customers> all = null;
		boolean all_already_persisted = false;
		MutableBoolean customer_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		// For role 'boughtOrder' in reference 'bought'. A->B Scenario
		customer_refilter = new MutableBoolean(false);
		Dataset<OrdersTDO> ordersTDOboughtboughtOrder = buyService.getOrdersTDOListBoughtOrderInBoughtInOrdersFromMongoDB(boughtOrder_condition, boughtOrder_refilter);
		Dataset<CustomersTDO> customersTDOboughtcustomer = buyService.getCustomersTDOListCustomerInBoughtInOrdersFromMongoDB(customer_condition, customer_refilter);
		if(customer_refilter.booleanValue()) {
			if(all == null)
				all = new CustomersServiceImpl().getCustomersList(customer_condition);
			joinCondition = null;
			joinCondition = customersTDOboughtcustomer.col("customerID").equalTo(all.col("customerID"));
			if(joinCondition == null)
				customersTDOboughtcustomer = customersTDOboughtcustomer.as("A").join(all).select("A.*").as(Encoders.bean(CustomersTDO.class));
			else
				customersTDOboughtcustomer = customersTDOboughtcustomer.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(CustomersTDO.class));
		}
	
		
		Dataset<Row> res_bought = ordersTDOboughtboughtOrder.join(customersTDOboughtcustomer
				.withColumnRenamed("customerID", "Customers_customerID")
				.withColumnRenamed("companyName", "Customers_companyName")
				.withColumnRenamed("contactName", "Customers_contactName")
				.withColumnRenamed("contactTitle", "Customers_contactTitle")
				.withColumnRenamed("address", "Customers_address")
				.withColumnRenamed("city", "Customers_city")
				.withColumnRenamed("region", "Customers_region")
				.withColumnRenamed("postalCode", "Customers_postalCode")
				.withColumnRenamed("country", "Customers_country")
				.withColumnRenamed("phone", "Customers_phone")
				.withColumnRenamed("fax", "Customers_fax")
				.withColumnRenamed("logEvents", "Customers_logEvents"),
				ordersTDOboughtboughtOrder.col("mongoDB_Orders_bought_CustomerRef").equalTo(customersTDOboughtcustomer.col("mongoDB_Orders_bought_custid")));
		Dataset<Orders> res_Orders_bought = res_bought.select( "id", "orderDate", "requiredDate", "shippedDate", "freight", "shipName", "shipAddress", "shipCity", "shipRegion", "shipPostalCode", "shipCountry", "logEvents").as(Encoders.bean(Orders.class));
		
		res_Orders_bought = res_Orders_bought.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Orders_bought);
		
		customer_refilter = new MutableBoolean(false);
		// For role 'customer' in reference 'purchases'  B->A Scenario
		Dataset<CustomersTDO> customersTDOpurchasescustomer = buyService.getCustomersTDOListCustomerInPurchasesInCustomersPurchasedFromKvDB(customer_condition, customer_refilter);
		Dataset<OrdersTDO> ordersTDOpurchasesboughtOrder = buyService.getOrdersTDOListBoughtOrderInPurchasesInCustomersPurchasedFromKvDB(boughtOrder_condition, boughtOrder_refilter);
		if(customer_refilter.booleanValue()) {
			if(all == null)
				all = new CustomersServiceImpl().getCustomersList(customer_condition);
			joinCondition = null;
			joinCondition = customersTDOpurchasescustomer.col("customerID").equalTo(all.col("customerID"));
			if(joinCondition == null)
				customersTDOpurchasescustomer = customersTDOpurchasescustomer.as("A").join(all).select("A.*").as(Encoders.bean(CustomersTDO.class));
			else
				customersTDOpurchasescustomer = customersTDOpurchasescustomer.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(CustomersTDO.class));
		}
		// Multi valued reference
		Dataset<Row> res_purchases = 
			customersTDOpurchasescustomer
			.withColumnRenamed("customerID", "Customers_customerID")
			.withColumnRenamed("companyName", "Customers_companyName")
			.withColumnRenamed("contactName", "Customers_contactName")
			.withColumnRenamed("contactTitle", "Customers_contactTitle")
			.withColumnRenamed("address", "Customers_address")
			.withColumnRenamed("city", "Customers_city")
			.withColumnRenamed("region", "Customers_region")
			.withColumnRenamed("postalCode", "Customers_postalCode")
			.withColumnRenamed("country", "Customers_country")
			.withColumnRenamed("phone", "Customers_phone")
			.withColumnRenamed("fax", "Customers_fax")
			.withColumnRenamed("logEvents", "Customers_logEvents")
			.join(ordersTDOpurchasesboughtOrder,
				functions.array_contains(customersTDOpurchasescustomer.col("kvDB_CustomersPurchased_purchases_orderref"),ordersTDOpurchasesboughtOrder.col("kvDB_CustomersPurchased_purchases_OrderID")));
		Dataset<Orders> res_Orders_purchases = res_purchases.select( "id", "orderDate", "requiredDate", "shippedDate", "freight", "shipName", "shipAddress", "shipCity", "shipRegion", "shipPostalCode", "shipCountry", "logEvents").as(Encoders.bean(Orders.class));
		res_Orders_purchases = res_Orders_purchases.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Orders_purchases);
		
		Dataset<Buy> res_buy_boughtOrder;
		Dataset<Orders> res_Orders;
		
		
		//Join datasets or return 
		Dataset<Orders> res = fullOuterJoinsOrders(datasetsPOJO);
		if(res == null)
			return null;
	
		if(boughtOrder_refilter.booleanValue())
			res = res.filter((FilterFunction<Orders>) r -> boughtOrder_condition == null || boughtOrder_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Orders> getProcessedOrderListInRegister(conditions.Condition<conditions.OrdersAttribute> processedOrder_condition,conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition)		{
		MutableBoolean processedOrder_refilter = new MutableBoolean(false);
		List<Dataset<Orders>> datasetsPOJO = new ArrayList<Dataset<Orders>>();
		Dataset<Employees> all = null;
		boolean all_already_persisted = false;
		MutableBoolean employeeInCharge_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		// For role 'processedOrder' in reference 'encoded'. A->B Scenario
		employeeInCharge_refilter = new MutableBoolean(false);
		Dataset<OrdersTDO> ordersTDOencodedprocessedOrder = registerService.getOrdersTDOListProcessedOrderInEncodedInOrdersFromMongoDB(processedOrder_condition, processedOrder_refilter);
		Dataset<EmployeesTDO> employeesTDOencodedemployeeInCharge = registerService.getEmployeesTDOListEmployeeInChargeInEncodedInOrdersFromMongoDB(employeeInCharge_condition, employeeInCharge_refilter);
		if(employeeInCharge_refilter.booleanValue()) {
			if(all == null)
				all = new EmployeesServiceImpl().getEmployeesList(employeeInCharge_condition);
			joinCondition = null;
			joinCondition = employeesTDOencodedemployeeInCharge.col("employeeID").equalTo(all.col("employeeID"));
			if(joinCondition == null)
				employeesTDOencodedemployeeInCharge = employeesTDOencodedemployeeInCharge.as("A").join(all).select("A.*").as(Encoders.bean(EmployeesTDO.class));
			else
				employeesTDOencodedemployeeInCharge = employeesTDOencodedemployeeInCharge.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(EmployeesTDO.class));
		}
	
		
		Dataset<Row> res_encoded = ordersTDOencodedprocessedOrder.join(employeesTDOencodedemployeeInCharge
				.withColumnRenamed("employeeID", "Employees_employeeID")
				.withColumnRenamed("lastName", "Employees_lastName")
				.withColumnRenamed("firstName", "Employees_firstName")
				.withColumnRenamed("title", "Employees_title")
				.withColumnRenamed("titleOfCourtesy", "Employees_titleOfCourtesy")
				.withColumnRenamed("birthDate", "Employees_birthDate")
				.withColumnRenamed("hireDate", "Employees_hireDate")
				.withColumnRenamed("address", "Employees_address")
				.withColumnRenamed("city", "Employees_city")
				.withColumnRenamed("region", "Employees_region")
				.withColumnRenamed("postalCode", "Employees_postalCode")
				.withColumnRenamed("country", "Employees_country")
				.withColumnRenamed("homePhone", "Employees_homePhone")
				.withColumnRenamed("extension", "Employees_extension")
				.withColumnRenamed("photo", "Employees_photo")
				.withColumnRenamed("notes", "Employees_notes")
				.withColumnRenamed("photoPath", "Employees_photoPath")
				.withColumnRenamed("salary", "Employees_salary")
				.withColumnRenamed("logEvents", "Employees_logEvents"),
				ordersTDOencodedprocessedOrder.col("mongoDB_Orders_encoded_EmployeeRef").equalTo(employeesTDOencodedemployeeInCharge.col("mongoDB_Orders_encoded_EmployeeID")));
		Dataset<Orders> res_Orders_encoded = res_encoded.select( "id", "orderDate", "requiredDate", "shippedDate", "freight", "shipName", "shipAddress", "shipCity", "shipRegion", "shipPostalCode", "shipCountry", "logEvents").as(Encoders.bean(Orders.class));
		
		res_Orders_encoded = res_Orders_encoded.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Orders_encoded);
		
		
		Dataset<Register> res_register_processedOrder;
		Dataset<Orders> res_Orders;
		
		
		//Join datasets or return 
		Dataset<Orders> res = fullOuterJoinsOrders(datasetsPOJO);
		if(res == null)
			return null;
	
		if(processedOrder_refilter.booleanValue())
			res = res.filter((FilterFunction<Orders>) r -> processedOrder_condition == null || processedOrder_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Orders> getShippedOrderListInShips(conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition,conditions.Condition<conditions.ShippersAttribute> shipper_condition)		{
		MutableBoolean shippedOrder_refilter = new MutableBoolean(false);
		List<Dataset<Orders>> datasetsPOJO = new ArrayList<Dataset<Orders>>();
		Dataset<Shippers> all = null;
		boolean all_already_persisted = false;
		MutableBoolean shipper_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		
		Dataset<Ships> res_ships_shippedOrder;
		Dataset<Orders> res_Orders;
		// Role 'shippedOrder' mapped to EmbeddedObject 'ShipmentInfo' - 'Shippers' containing 'Orders'
		shipper_refilter = new MutableBoolean(false);
		res_ships_shippedOrder = shipsService.getShipsListInmongoDBOrdersShipmentInfo(shippedOrder_condition, shipper_condition, shippedOrder_refilter, shipper_refilter);
		if(shipper_refilter.booleanValue()) {
			if(all == null)
				all = new ShippersServiceImpl().getShippersList(shipper_condition);
			joinCondition = null;
			joinCondition = res_ships_shippedOrder.col("shipper.shipperID").equalTo(all.col("shipperID"));
			if(joinCondition == null)
				res_Orders = res_ships_shippedOrder.join(all).select("shippedOrder.*").as(Encoders.bean(Orders.class));
			else
				res_Orders = res_ships_shippedOrder.join(all, joinCondition).select("shippedOrder.*").as(Encoders.bean(Orders.class));
		
		} else
			res_Orders = res_ships_shippedOrder.map((MapFunction<Ships,Orders>) r -> r.getShippedOrder(), Encoders.bean(Orders.class));
		res_Orders = res_Orders.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Orders);
		
		
		//Join datasets or return 
		Dataset<Orders> res = fullOuterJoinsOrders(datasetsPOJO);
		if(res == null)
			return null;
	
		if(shippedOrder_refilter.booleanValue())
			res = res.filter((FilterFunction<Orders>) r -> shippedOrder_condition == null || shippedOrder_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Orders> getOrderListInComposedOf(conditions.Condition<conditions.OrdersAttribute> order_condition,conditions.Condition<conditions.ProductsAttribute> orderedProducts_condition, conditions.Condition<conditions.ComposedOfAttribute> composedOf_condition)		{
		MutableBoolean order_refilter = new MutableBoolean(false);
		List<Dataset<Orders>> datasetsPOJO = new ArrayList<Dataset<Orders>>();
		Dataset<Products> all = null;
		boolean all_already_persisted = false;
		MutableBoolean orderedProducts_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		// join physical structure A<-AB->B
		
		//join between 2 SQL tables and a non-relational structure
		// (A) (AB - B)
		orderedProducts_refilter = new MutableBoolean(false);
		MutableBoolean composedOf_refilter = new MutableBoolean(false);
		Dataset<ComposedOfTDO> res_composedOf_purchasedProducts_order = composedOfService.getComposedOfTDOListInProductsAndOrder_DetailsFrommyRelDB(orderedProducts_condition, composedOf_condition, orderedProducts_refilter, composedOf_refilter);
		Dataset<OrdersTDO> res_order_purchasedProducts = composedOfService.getOrdersTDOListOrderInOrderInOrdersFromMongoDB(order_condition, order_refilter);
		if(orderedProducts_refilter.booleanValue()) {
			if(all == null)
					all = new ProductsServiceImpl().getProductsList(orderedProducts_condition);
			joinCondition = null;
				joinCondition = res_composedOf_purchasedProducts_order.col("orderedProducts.productId").equalTo(all.col("productId"));
				res_composedOf_purchasedProducts_order = res_composedOf_purchasedProducts_order.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(ComposedOfTDO.class));
		} 
		Dataset<Row> res_row_purchasedProducts_order = res_composedOf_purchasedProducts_order.join(res_order_purchasedProducts.withColumnRenamed("logEvents", "composedOf_logEvents"),
			res_composedOf_purchasedProducts_order.col("relDB_Order_Details_order_OrderRef").equalTo(res_order_purchasedProducts.col("relDB_Order_Details_order_OrderID")));
		Dataset<Orders> res_Orders_order = res_row_purchasedProducts_order.as(Encoders.bean(Orders.class));
		datasetsPOJO.add(res_Orders_order.dropDuplicates(new String[] {"id"}));	
		
		
		
		Dataset<ComposedOf> res_composedOf_order;
		Dataset<Orders> res_Orders;
		
		
		//Join datasets or return 
		Dataset<Orders> res = fullOuterJoinsOrders(datasetsPOJO);
		if(res == null)
			return null;
	
		if(order_refilter.booleanValue())
			res = res.filter((FilterFunction<Orders>) r -> order_condition == null || order_condition.evaluate(r));
		
	
		return res;
		}
	
	public boolean insertOrders(
		Orders orders,
		Customers	customerBuy,
		Employees	employeeInChargeRegister,
		Shippers	shipperShips){
			boolean inserted = false;
			// Insert in standalone structures
			// Insert in structures containing double embedded role
			// Insert in descending structures
			inserted = insertOrdersInOrdersFromMyMongoDB(orders,customerBuy,employeeInChargeRegister,shipperShips)|| inserted ;
			// Insert in ascending structures 
			// Insert in ref structures 
			// Insert in ref structures mapped to opposite role of mandatory role  
			inserted = insertOrdersInCustomersPurchasedFromMyRedisDB(orders,customerBuy,employeeInChargeRegister,shipperShips)|| inserted ;
			return inserted;
		}
	
	public boolean insertOrdersInOrdersFromMyMongoDB(Orders orders,
		Customers	customerBuy,
		Employees	employeeInChargeRegister,
		Shippers	shipperShips)	{
			 // Implement Insert in descending complex struct
			Bson filter = new Document();
			Bson updateOp;
			Document docOrders_1 = new Document();
			docOrders_1.append("OrderID",orders.getId());
			docOrders_1.append("OrderDate",orders.getOrderDate());
			docOrders_1.append("RequiredDate",orders.getRequiredDate());
			docOrders_1.append("Freight",orders.getFreight());
			// field 'ShipmentInfo' is mapped to mandatory role 'shippedOrder' with opposite role of type 'Shippers'
					Shippers shippers = shipperShips;
					Document docShipmentInfo_2 = new Document();
					docShipmentInfo_2.append("ShipperID",shippers.getShipperID());
					docShipmentInfo_2.append("CompanyName",shippers.getCompanyName());
					docShipmentInfo_2.append("Phone",shippers.getPhone());
					
						// Embedded attributes of 'Orders' in 'ShipmentInfo' which is also mapped to a role.
						
						docShipmentInfo_2.append("ShippedDate",orders.getShippedDate());
						docShipmentInfo_2.append("ShipName",orders.getShipName());
						docShipmentInfo_2.append("ShipAddress",orders.getShipAddress());
						docShipmentInfo_2.append("ShipCity",orders.getShipCity());
						docShipmentInfo_2.append("ShipRegion",orders.getShipRegion());
						docShipmentInfo_2.append("ShipPostalCode",orders.getShipPostalCode());
						docShipmentInfo_2.append("ShipCountry",orders.getShipCountry());
						
					docOrders_1.append("ShipmentInfo", docShipmentInfo_2);
			// Ref 'bought' mapped to mandatory role 'boughtOrder'
			docOrders_1.append("CustomerRef",customerBuy.getCustomerID());
			// Ref 'encoded' mapped to mandatory role 'processedOrder'
			docOrders_1.append("EmployeeRef",employeeInChargeRegister.getEmployeeID());
			
			filter = eq("OrderID",orders.getId());
			updateOp = setOnInsert(docOrders_1);
			DBConnectionMgr.upsertMany(filter, updateOp, "Orders", "myMongoDB");
			return true;
		}
	public boolean insertOrdersInCustomersPurchasedFromMyRedisDB(Orders orders,
		Customers	customerBuy,
		Employees	employeeInChargeRegister,
		Shippers	shipperShips)	{
		 	// Entity 'Orders' reference in structure 'CustomersPurchased'
			// In Key value database
			Customers customers = customerBuy;
			// Build key
				String key="";
				String value="";
				key += "CUSTOMER:";
				key += customers.getCustomerID();
				key += ":ORDERS";
				// Value part in List
				value =  Util.getStringValue(orders.getId());
				DBConnectionMgr.writeKeyValueList(key,value,"myRedisDB");
			return false;
						
			
		}
	private boolean inUpdateMethod = false;
	private List<Row> allOrdersIdList = null;
	public void updateOrdersList(conditions.Condition<conditions.OrdersAttribute> condition, conditions.SetClause<conditions.OrdersAttribute> set){
		inUpdateMethod = true;
		try {
	
	
		} finally {
			inUpdateMethod = false;
		}
	}
	
	
	
	
	
	public void updateOrders(pojo.Orders orders) {
		//TODO using the id
		return;
	}
	public void updateBoughtOrderListInBuy(
		conditions.Condition<conditions.OrdersAttribute> boughtOrder_condition,
		conditions.Condition<conditions.CustomersAttribute> customer_condition,
		
		conditions.SetClause<conditions.OrdersAttribute> set
	){
		//TODO
	}
	
	public void updateBoughtOrderListInBuyByBoughtOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> boughtOrder_condition,
		conditions.SetClause<conditions.OrdersAttribute> set
	){
		updateBoughtOrderListInBuy(boughtOrder_condition, null, set);
	}
	public void updateBoughtOrderListInBuyByCustomerCondition(
		conditions.Condition<conditions.CustomersAttribute> customer_condition,
		conditions.SetClause<conditions.OrdersAttribute> set
	){
		updateBoughtOrderListInBuy(null, customer_condition, set);
	}
	
	public void updateBoughtOrderListInBuyByCustomer(
		pojo.Customers customer,
		conditions.SetClause<conditions.OrdersAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateProcessedOrderListInRegister(
		conditions.Condition<conditions.OrdersAttribute> processedOrder_condition,
		conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition,
		
		conditions.SetClause<conditions.OrdersAttribute> set
	){
		//TODO
	}
	
	public void updateProcessedOrderListInRegisterByProcessedOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> processedOrder_condition,
		conditions.SetClause<conditions.OrdersAttribute> set
	){
		updateProcessedOrderListInRegister(processedOrder_condition, null, set);
	}
	public void updateProcessedOrderListInRegisterByEmployeeInChargeCondition(
		conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition,
		conditions.SetClause<conditions.OrdersAttribute> set
	){
		updateProcessedOrderListInRegister(null, employeeInCharge_condition, set);
	}
	
	public void updateProcessedOrderListInRegisterByEmployeeInCharge(
		pojo.Employees employeeInCharge,
		conditions.SetClause<conditions.OrdersAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateShippedOrderListInShips(
		conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition,
		conditions.Condition<conditions.ShippersAttribute> shipper_condition,
		
		conditions.SetClause<conditions.OrdersAttribute> set
	){
		//TODO
	}
	
	public void updateShippedOrderListInShipsByShippedOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition,
		conditions.SetClause<conditions.OrdersAttribute> set
	){
		updateShippedOrderListInShips(shippedOrder_condition, null, set);
	}
	public void updateShippedOrderListInShipsByShipperCondition(
		conditions.Condition<conditions.ShippersAttribute> shipper_condition,
		conditions.SetClause<conditions.OrdersAttribute> set
	){
		updateShippedOrderListInShips(null, shipper_condition, set);
	}
	
	public void updateShippedOrderListInShipsByShipper(
		pojo.Shippers shipper,
		conditions.SetClause<conditions.OrdersAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateOrderListInComposedOf(
		conditions.Condition<conditions.OrdersAttribute> order_condition,
		conditions.Condition<conditions.ProductsAttribute> orderedProducts_condition,
		conditions.Condition<conditions.ComposedOfAttribute> composedOf,
		conditions.SetClause<conditions.OrdersAttribute> set
	){
		//TODO
	}
	
	public void updateOrderListInComposedOfByOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> order_condition,
		conditions.SetClause<conditions.OrdersAttribute> set
	){
		updateOrderListInComposedOf(order_condition, null, null, set);
	}
	public void updateOrderListInComposedOfByOrderedProductsCondition(
		conditions.Condition<conditions.ProductsAttribute> orderedProducts_condition,
		conditions.SetClause<conditions.OrdersAttribute> set
	){
		updateOrderListInComposedOf(null, orderedProducts_condition, null, set);
	}
	
	public void updateOrderListInComposedOfByOrderedProducts(
		pojo.Products orderedProducts,
		conditions.SetClause<conditions.OrdersAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateOrderListInComposedOfByComposedOfCondition(
		conditions.Condition<conditions.ComposedOfAttribute> composedOf_condition,
		conditions.SetClause<conditions.OrdersAttribute> set
	){
		updateOrderListInComposedOf(null, null, composedOf_condition, set);
	}
	
	
	public void deleteOrdersList(conditions.Condition<conditions.OrdersAttribute> condition){
		//TODO
	}
	
	public void deleteOrders(pojo.Orders orders) {
		//TODO using the id
		return;
	}
	public void deleteBoughtOrderListInBuy(	
		conditions.Condition<conditions.OrdersAttribute> boughtOrder_condition,	
		conditions.Condition<conditions.CustomersAttribute> customer_condition){
			//TODO
		}
	
	public void deleteBoughtOrderListInBuyByBoughtOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> boughtOrder_condition
	){
		deleteBoughtOrderListInBuy(boughtOrder_condition, null);
	}
	public void deleteBoughtOrderListInBuyByCustomerCondition(
		conditions.Condition<conditions.CustomersAttribute> customer_condition
	){
		deleteBoughtOrderListInBuy(null, customer_condition);
	}
	
	public void deleteBoughtOrderListInBuyByCustomer(
		pojo.Customers customer 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteProcessedOrderListInRegister(	
		conditions.Condition<conditions.OrdersAttribute> processedOrder_condition,	
		conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition){
			//TODO
		}
	
	public void deleteProcessedOrderListInRegisterByProcessedOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> processedOrder_condition
	){
		deleteProcessedOrderListInRegister(processedOrder_condition, null);
	}
	public void deleteProcessedOrderListInRegisterByEmployeeInChargeCondition(
		conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition
	){
		deleteProcessedOrderListInRegister(null, employeeInCharge_condition);
	}
	
	public void deleteProcessedOrderListInRegisterByEmployeeInCharge(
		pojo.Employees employeeInCharge 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteShippedOrderListInShips(	
		conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition,	
		conditions.Condition<conditions.ShippersAttribute> shipper_condition){
			//TODO
		}
	
	public void deleteShippedOrderListInShipsByShippedOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition
	){
		deleteShippedOrderListInShips(shippedOrder_condition, null);
	}
	public void deleteShippedOrderListInShipsByShipperCondition(
		conditions.Condition<conditions.ShippersAttribute> shipper_condition
	){
		deleteShippedOrderListInShips(null, shipper_condition);
	}
	
	public void deleteShippedOrderListInShipsByShipper(
		pojo.Shippers shipper 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteOrderListInComposedOf(	
		conditions.Condition<conditions.OrdersAttribute> order_condition,	
		conditions.Condition<conditions.ProductsAttribute> orderedProducts_condition,
		conditions.Condition<conditions.ComposedOfAttribute> composedOf){
			//TODO
		}
	
	public void deleteOrderListInComposedOfByOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> order_condition
	){
		deleteOrderListInComposedOf(order_condition, null, null);
	}
	public void deleteOrderListInComposedOfByOrderedProductsCondition(
		conditions.Condition<conditions.ProductsAttribute> orderedProducts_condition
	){
		deleteOrderListInComposedOf(null, orderedProducts_condition, null);
	}
	
	public void deleteOrderListInComposedOfByOrderedProducts(
		pojo.Products orderedProducts 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteOrderListInComposedOfByComposedOfCondition(
		conditions.Condition<conditions.ComposedOfAttribute> composedOf_condition
	){
		deleteOrderListInComposedOf(null, null, composedOf_condition);
	}
	
}
