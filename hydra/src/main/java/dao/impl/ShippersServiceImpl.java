package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Shippers;
import conditions.*;
import dao.services.ShippersService;
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


public class ShippersServiceImpl extends ShippersService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ShippersServiceImpl.class);
	
	
	
	
	
	
	public static Pair<List<String>, List<String>> getBSONUpdateQueryInOrdersFromMyMongoDB(conditions.SetClause<ShippersAttribute> set) {
		List<String> res = new ArrayList<String>();
		Set<String> arrayFields = new HashSet<String>();
		if(set != null) {
			java.util.Map<String, java.util.Map<String, String>> longFieldValues = new java.util.HashMap<String, java.util.Map<String, String>>();
			java.util.Map<ShippersAttribute, Object> clause = set.getClause();
			for(java.util.Map.Entry<ShippersAttribute, Object> e : clause.entrySet()) {
				ShippersAttribute attr = e.getKey();
				Object value = e.getValue();
				if(attr == ShippersAttribute.shipperID ) {
					String fieldName = "ShipperID";
					fieldName = "ShipmentInfo." + fieldName;
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ShippersAttribute.companyName ) {
					String fieldName = "CompanyName";
					fieldName = "ShipmentInfo." + fieldName;
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ShippersAttribute.phone ) {
					String fieldName = "Phone";
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
	
	public static String getBSONMatchQueryInOrdersFromMyMongoDB(Condition<ShippersAttribute> condition, MutableBoolean refilterFlag) {	
		String res = null;	
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				ShippersAttribute attr = ((SimpleCondition<ShippersAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<ShippersAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<ShippersAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == ShippersAttribute.shipperID ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "ShipperID': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "ShipmentInfo." + res;
					res = "'" + res;
					}
					if(attr == ShippersAttribute.companyName ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "CompanyName': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "ShipmentInfo." + res;
					res = "'" + res;
					}
					if(attr == ShippersAttribute.phone ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "Phone': {" + mongoOp + ": " + preparedValue + "}";
	
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
	
	public static Pair<String, List<String>> getBSONQueryAndArrayFilterForUpdateQueryInOrdersFromMyMongoDB(Condition<ShippersAttribute> condition, final List<String> arrayVariableNames, Set<String> arrayVariablesUsed, MutableBoolean refilterFlag) {	
		String query = null;
		List<String> arrayFilters = new ArrayList<String>();
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				String bson = null;
				ShippersAttribute attr = ((SimpleCondition<ShippersAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<ShippersAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<ShippersAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == ShippersAttribute.shipperID ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "ShipperID': {" + mongoOp + ": " + preparedValue + "}";
					
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
					if(attr == ShippersAttribute.companyName ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "CompanyName': {" + mongoOp + ": " + preparedValue + "}";
					
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
					if(attr == ShippersAttribute.phone ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "Phone': {" + mongoOp + ": " + preparedValue + "}";
					
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
	
	
	
	public Dataset<Shippers> getShippersListInOrdersFromMyMongoDB(conditions.Condition<conditions.ShippersAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = ShippersServiceImpl.getBSONMatchQueryInOrdersFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Orders", bsonQuery);
	
		Dataset<Shippers> res = dataset.flatMap((FlatMapFunction<Row, Shippers>) r -> {
				Set<Shippers> list_res = new HashSet<Shippers>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				Shippers shippers1 = new Shippers();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Shippers.shipperID for field ShipperID			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipperID")) {
						if(nestedRow.getAs("ShipperID")==null)
							shippers1.setShipperID(null);
						else{
							shippers1.setShipperID(Util.getIntegerValue(nestedRow.getAs("ShipperID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Shippers.companyName for field CompanyName			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("CompanyName")) {
						if(nestedRow.getAs("CompanyName")==null)
							shippers1.setCompanyName(null);
						else{
							shippers1.setCompanyName(Util.getStringValue(nestedRow.getAs("CompanyName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Shippers.phone for field Phone			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Phone")) {
						if(nestedRow.getAs("Phone")==null)
							shippers1.setPhone(null);
						else{
							shippers1.setPhone(Util.getStringValue(nestedRow.getAs("Phone")));
							toAdd1 = true;					
							}
					}
					if(toAdd1) {
						list_res.add(shippers1);
						addedInList = true;
					} 
					
				
				return list_res.iterator();
	
		}, Encoders.bean(Shippers.class));
		res= res.dropDuplicates(new String[]{"shipperID"});
		return res;
		
	}
	
	
	
	
	
	
	public Dataset<Shippers> getShipperListInShips(conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition,conditions.Condition<conditions.ShippersAttribute> shipper_condition)		{
		MutableBoolean shipper_refilter = new MutableBoolean(false);
		List<Dataset<Shippers>> datasetsPOJO = new ArrayList<Dataset<Shippers>>();
		Dataset<Orders> all = null;
		boolean all_already_persisted = false;
		MutableBoolean shippedOrder_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		
		Dataset<Ships> res_ships_shipper;
		Dataset<Shippers> res_Shippers;
		// Role 'shippedOrder' mapped to EmbeddedObject 'ShipmentInfo' 'Shippers' containing 'Orders' 
		shippedOrder_refilter = new MutableBoolean(false);
		res_ships_shipper = shipsService.getShipsListInmongoDBOrdersShipmentInfo(shippedOrder_condition, shipper_condition, shippedOrder_refilter, shipper_refilter);
		if(shippedOrder_refilter.booleanValue()) {
			if(all == null)
				all = new OrdersServiceImpl().getOrdersList(shippedOrder_condition);
			joinCondition = null;
			joinCondition = res_ships_shipper.col("shippedOrder.id").equalTo(all.col("id"));
			if(joinCondition == null)
				res_Shippers = res_ships_shipper.join(all).select("shipper.*").as(Encoders.bean(Shippers.class));
			else
				res_Shippers = res_ships_shipper.join(all, joinCondition).select("shipper.*").as(Encoders.bean(Shippers.class));
		
		} else
			res_Shippers = res_ships_shipper.map((MapFunction<Ships,Shippers>) r -> r.getShipper(), Encoders.bean(Shippers.class));
		res_Shippers = res_Shippers.dropDuplicates(new String[] {"shipperID"});
		datasetsPOJO.add(res_Shippers);
		
		
		//Join datasets or return 
		Dataset<Shippers> res = fullOuterJoinsShippers(datasetsPOJO);
		if(res == null)
			return null;
	
		if(shipper_refilter.booleanValue())
			res = res.filter((FilterFunction<Shippers>) r -> shipper_condition == null || shipper_condition.evaluate(r));
		
	
		return res;
		}
	
	
	public boolean insertShippers(Shippers shippers){
		// Insert into all mapped standalone AbstractPhysicalStructure 
		boolean inserted = false;
		return inserted;
	}
	
	private boolean inUpdateMethod = false;
	private List<Row> allShippersIdList = null;
	public void updateShippersList(conditions.Condition<conditions.ShippersAttribute> condition, conditions.SetClause<conditions.ShippersAttribute> set){
		inUpdateMethod = true;
		try {
	
	
		} finally {
			inUpdateMethod = false;
		}
	}
	
	
	
	
	
	public void updateShippers(pojo.Shippers shippers) {
		//TODO using the id
		return;
	}
	public void updateShipperListInShips(
		conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition,
		conditions.Condition<conditions.ShippersAttribute> shipper_condition,
		
		conditions.SetClause<conditions.ShippersAttribute> set
	){
		//TODO
	}
	
	public void updateShipperListInShipsByShippedOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition,
		conditions.SetClause<conditions.ShippersAttribute> set
	){
		updateShipperListInShips(shippedOrder_condition, null, set);
	}
	
	public void updateShipperInShipsByShippedOrder(
		pojo.Orders shippedOrder,
		conditions.SetClause<conditions.ShippersAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateShipperListInShipsByShipperCondition(
		conditions.Condition<conditions.ShippersAttribute> shipper_condition,
		conditions.SetClause<conditions.ShippersAttribute> set
	){
		updateShipperListInShips(null, shipper_condition, set);
	}
	
	
	public void deleteShippersList(conditions.Condition<conditions.ShippersAttribute> condition){
		//TODO
	}
	
	public void deleteShippers(pojo.Shippers shippers) {
		//TODO using the id
		return;
	}
	public void deleteShipperListInShips(	
		conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition,	
		conditions.Condition<conditions.ShippersAttribute> shipper_condition){
			//TODO
		}
	
	public void deleteShipperListInShipsByShippedOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition
	){
		deleteShipperListInShips(shippedOrder_condition, null);
	}
	
	public void deleteShipperInShipsByShippedOrder(
		pojo.Orders shippedOrder 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteShipperListInShipsByShipperCondition(
		conditions.Condition<conditions.ShippersAttribute> shipper_condition
	){
		deleteShipperListInShips(null, shipper_condition);
	}
	
}
