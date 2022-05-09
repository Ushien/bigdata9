package dao.impl;

import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import org.apache.commons.lang3.StringUtils;
import util.Dataset;
import conditions.Condition;
import java.util.HashSet;
import java.util.Set;
import conditions.AndCondition;
import conditions.OrCondition;
import conditions.SimpleCondition;
import conditions.BuyAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.OrdersTDO;
import tdo.BuyTDO;
import conditions.OrdersAttribute;
import dao.services.OrdersService;
import tdo.CustomersTDO;
import tdo.BuyTDO;
import conditions.CustomersAttribute;
import dao.services.CustomersService;
import java.util.List;
import java.util.ArrayList;
import util.ScalaUtil;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.commons.lang.mutable.MutableBoolean;
import util.Util;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import util.Row;
import org.apache.spark.sql.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import util.WrappedArray;
import org.apache.spark.api.java.function.FlatMapFunction;
import dbconnection.SparkConnectionMgr;
import dbconnection.DBConnectionMgr;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.ArrayType;
import static com.mongodb.client.model.Updates.addToSet;
import org.bson.Document;
import org.bson.conversions.Bson;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;

public class BuyServiceImpl extends dao.services.BuyService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BuyServiceImpl.class);
	
	
	// Left side 'CustomerRef' of reference [bought ]
	public Dataset<OrdersTDO> getOrdersTDOListBoughtOrderInBoughtInOrdersFromMongoDB(Condition<OrdersAttribute> condition, MutableBoolean refilterFlag){	
		String bsonQuery = OrdersServiceImpl.getBSONMatchQueryInOrdersFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Orders", bsonQuery);
	
		Dataset<OrdersTDO> res = dataset.flatMap((FlatMapFunction<Row, OrdersTDO>) r -> {
				Set<OrdersTDO> list_res = new HashSet<OrdersTDO>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				OrdersTDO orders1 = new OrdersTDO();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Orders.id for field OrderID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderID")) {
						if(nestedRow.getAs("OrderID") == null){
							orders1.setId(null);
						}else{
							orders1.setId(Util.getIntegerValue(nestedRow.getAs("OrderID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.orderDate for field OrderDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderDate")) {
						if(nestedRow.getAs("OrderDate") == null){
							orders1.setOrderDate(null);
						}else{
							orders1.setOrderDate(Util.getLocalDateValue(nestedRow.getAs("OrderDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.requiredDate for field RequiredDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("RequiredDate")) {
						if(nestedRow.getAs("RequiredDate") == null){
							orders1.setRequiredDate(null);
						}else{
							orders1.setRequiredDate(Util.getLocalDateValue(nestedRow.getAs("RequiredDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.freight for field Freight			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Freight")) {
						if(nestedRow.getAs("Freight") == null){
							orders1.setFreight(null);
						}else{
							orders1.setFreight(Util.getDoubleValue(nestedRow.getAs("Freight")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shippedDate for field ShippedDate			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShippedDate")) {
						if(nestedRow.getAs("ShippedDate") == null){
							orders1.setShippedDate(null);
						}else{
							orders1.setShippedDate(Util.getLocalDateValue(nestedRow.getAs("ShippedDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipName for field ShipName			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipName")) {
						if(nestedRow.getAs("ShipName") == null){
							orders1.setShipName(null);
						}else{
							orders1.setShipName(Util.getStringValue(nestedRow.getAs("ShipName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipAddress for field ShipAddress			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipAddress")) {
						if(nestedRow.getAs("ShipAddress") == null){
							orders1.setShipAddress(null);
						}else{
							orders1.setShipAddress(Util.getStringValue(nestedRow.getAs("ShipAddress")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipCity for field ShipCity			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCity")) {
						if(nestedRow.getAs("ShipCity") == null){
							orders1.setShipCity(null);
						}else{
							orders1.setShipCity(Util.getStringValue(nestedRow.getAs("ShipCity")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipRegion for field ShipRegion			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipRegion")) {
						if(nestedRow.getAs("ShipRegion") == null){
							orders1.setShipRegion(null);
						}else{
							orders1.setShipRegion(Util.getStringValue(nestedRow.getAs("ShipRegion")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipPostalCode for field ShipPostalCode			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipPostalCode")) {
						if(nestedRow.getAs("ShipPostalCode") == null){
							orders1.setShipPostalCode(null);
						}else{
							orders1.setShipPostalCode(Util.getStringValue(nestedRow.getAs("ShipPostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipCountry for field ShipCountry			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCountry")) {
						if(nestedRow.getAs("ShipCountry") == null){
							orders1.setShipCountry(null);
						}else{
							orders1.setShipCountry(Util.getStringValue(nestedRow.getAs("ShipCountry")));
							toAdd1 = true;					
							}
					}
					
						// field  CustomerRef for reference bought . Reference field : CustomerRef
					nestedRow =  r1;
					if(nestedRow != null) {
						orders1.setMongoDB_Orders_bought_CustomerRef(nestedRow.getAs("CustomerRef") == null ? null : nestedRow.getAs("CustomerRef").toString());
						toAdd1 = true;					
					}
					
					
					if(toAdd1) {
						list_res.add(orders1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(OrdersTDO.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
	}
	
	// Right side 'custid' of reference [bought ]
	public Dataset<CustomersTDO> getCustomersTDOListCustomerInBoughtInOrdersFromMongoDB(Condition<CustomersAttribute> condition, MutableBoolean refilterFlag){
		// Build the key pattern
		//  - If the condition attribute is in the key pattern, replace by the value. Only if operator is EQUALS.
		//  - Replace all other fields of key pattern by a '*' 
		String keypattern= "", keypatternAllVariables="";
		String valueCond=null;
		String finalKeypattern;
		List<String> fieldsListInKey = new ArrayList<>();
		Set<CustomersAttribute> keyAttributes = new HashSet<>();
		keypattern=keypattern.concat("CUSTOMER:");
		keypatternAllVariables=keypatternAllVariables.concat("CUSTOMER:");
		if(!Util.containsOrCondition(condition)){
			valueCond=Util.getStringValue(Util.getValueOfAttributeInEqualCondition(condition,CustomersAttribute.customerID));
			keyAttributes.add(CustomersAttribute.customerID);
		}
		else{
			valueCond=null;
			refilterFlag.setValue(true);
		}
		if(valueCond==null)
			keypattern=keypattern.concat("*");
		else
			keypattern=keypattern.concat(valueCond);
		fieldsListInKey.add("custid");
		keypatternAllVariables=keypatternAllVariables.concat("*");
		if(!refilterFlag.booleanValue()){
			Set<CustomersAttribute> conditionAttributes = Util.getConditionAttributes(condition);
			for (CustomersAttribute a : conditionAttributes) {
				if (!keyAttributes.contains(a)) {
					refilterFlag.setValue(true);
					break;
				}
			}
		}
	
			
		// Find the type of query to perform in order to retrieve a Dataset<Row>
		// Based on the type of the value. Is a it a simple string or a hash or a list... 
		Dataset<Row> rows;
		StructType structType = new StructType(new StructField[] {
			DataTypes.createStructField("_id", DataTypes.StringType, true), //technical field to store the key.
			DataTypes.createStructField("CompanyName", DataTypes.StringType, true)
	,		DataTypes.createStructField("ContactName", DataTypes.StringType, true)
	,		DataTypes.createStructField("ContactTitle", DataTypes.StringType, true)
	,		DataTypes.createStructField("Address", DataTypes.StringType, true)
	,		DataTypes.createStructField("City", DataTypes.StringType, true)
	,		DataTypes.createStructField("Region", DataTypes.StringType, true)
	,		DataTypes.createStructField("PostalCode", DataTypes.StringType, true)
	,		DataTypes.createStructField("Country", DataTypes.StringType, true)
	,		DataTypes.createStructField("Phone", DataTypes.StringType, true)
	,		DataTypes.createStructField("Fax", DataTypes.StringType, true)
		});
		rows = SparkConnectionMgr.getRowsFromKeyValueHashes("myRedisDB",keypattern, structType);
		if(rows == null || rows.isEmpty())
				return null;
		boolean isStriped = false;
		String prefix=isStriped?keypattern.substring(0, keypattern.length() - 1):"";
		finalKeypattern = keypatternAllVariables;
		Dataset<CustomersTDO> res = rows.map((MapFunction<Row, CustomersTDO>) r -> {
					CustomersTDO customers_res = new CustomersTDO();
					Integer groupindex = null;
					String regex = null;
					String value = null;
					Pattern p, pattern = null;
					Matcher m, match = null;
					boolean matches = false;
					String key = isStriped ? prefix + r.getAs("_id") : r.getAs("_id");
					// Spark Redis automatically strips leading character if the pattern provided contains a single '*' at the end.				
					pattern = Pattern.compile("\\*");
			        match = pattern.matcher(finalKeypattern);
					regex = finalKeypattern.replaceAll("\\*","(.*)");
					p = Pattern.compile(regex);
					m = p.matcher(key);
					matches = m.find();
					// attribute [Customers.CustomerID]
					// Attribute mapped in a key.
					groupindex = fieldsListInKey.indexOf("custid")+1;
					if(groupindex==null) {
						logger.warn("Attribute 'Customers' mapped physical field 'custid' found in key but can't get index in build keypattern '{}'.", finalKeypattern);
					}
					String customerID = null;
					if(matches) {
						customerID = m.group(groupindex.intValue());
					} else {
						logger.warn("Cannot retrieve value for CustomerscustomerID attribute stored in db myRedisDB. Regex [{}] Value [{}]",regex,value);
						customers_res.addLogEvent("Cannot retrieve value for Customers.customerID attribute stored in db myRedisDB. Probably due to an ambiguous regex.");
					}
					customers_res.setCustomerID(customerID == null ? null : customerID);
					// attribute [Customers.CompanyName]
					String companyName = r.getAs("CompanyName") == null ? null : r.getAs("CompanyName");
					customers_res.setCompanyName(companyName);
					// attribute [Customers.ContactName]
					String contactName = r.getAs("ContactName") == null ? null : r.getAs("ContactName");
					customers_res.setContactName(contactName);
					// attribute [Customers.ContactTitle]
					String contactTitle = r.getAs("ContactTitle") == null ? null : r.getAs("ContactTitle");
					customers_res.setContactTitle(contactTitle);
					// attribute [Customers.Address]
					String address = r.getAs("Address") == null ? null : r.getAs("Address");
					customers_res.setAddress(address);
					// attribute [Customers.City]
					String city = r.getAs("City") == null ? null : r.getAs("City");
					customers_res.setCity(city);
					// attribute [Customers.Region]
					String region = r.getAs("Region") == null ? null : r.getAs("Region");
					customers_res.setRegion(region);
					// attribute [Customers.PostalCode]
					String postalCode = r.getAs("PostalCode") == null ? null : r.getAs("PostalCode");
					customers_res.setPostalCode(postalCode);
					// attribute [Customers.Country]
					String country = r.getAs("Country") == null ? null : r.getAs("Country");
					customers_res.setCountry(country);
					// attribute [Customers.Phone]
					String phone = r.getAs("Phone") == null ? null : r.getAs("Phone");
					customers_res.setPhone(phone);
					// attribute [Customers.Fax]
					String fax = r.getAs("Fax") == null ? null : r.getAs("Fax");
					customers_res.setFax(fax);
					//Checking that reference field 'custid' is mapped in Key
					if(fieldsListInKey.contains("custid")){
						//Retrieving reference field 'custid' in Key
						Pattern pattern_custid = Pattern.compile("\\*");
				        Matcher match_custid = pattern_custid.matcher(finalKeypattern);
						regex = finalKeypattern.replaceAll("\\*","(.*)");
						groupindex = fieldsListInKey.indexOf("custid")+1;
						if(groupindex==null) {
							logger.warn("Attribute 'Customers' mapped physical field 'custid' found in key but can't get index in build keypattern '{}'.", finalKeypattern);
						}
						p = Pattern.compile(regex);
						m = p.matcher(key);
						matches = m.find();
						String bought_custid = null;
						if(matches) {
						bought_custid = m.group(groupindex.intValue());
						} else {
						logger.warn("Cannot retrieve value 'custid'. Regex [{}] Value [{}]",regex,value);
						customers_res.addLogEvent("Cannot retrieve value for 'custid' attribute stored in db myRedisDB. Probably due to an ambiguous regex.");
						}
						customers_res.setMongoDB_Orders_bought_custid(bought_custid);
					}
	
						return customers_res;
				}, Encoders.bean(CustomersTDO.class));
		res=res.dropDuplicates(new String[] {"customerID"});
		return res;
	}
	
	
	
	// Left side 'orderref' of reference [purchases ]
	public Dataset<CustomersTDO> getCustomersTDOListCustomerInPurchasesInCustomersPurchasedFromKvDB(Condition<CustomersAttribute> condition, MutableBoolean refilterFlag){	
		// Build the key pattern
		//  - If the condition attribute is in the key pattern, replace by the value. Only if operator is EQUALS.
		//  - Replace all other fields of key pattern by a '*' 
		String keypattern= "", keypatternAllVariables="";
		String valueCond=null;
		String finalKeypattern;
		List<String> fieldsListInKey = new ArrayList<>();
		Set<CustomersAttribute> keyAttributes = new HashSet<>();
		keypattern=keypattern.concat("CUSTOMER:");
		keypatternAllVariables=keypatternAllVariables.concat("CUSTOMER:");
		if(!Util.containsOrCondition(condition)){
			valueCond=Util.getStringValue(Util.getValueOfAttributeInEqualCondition(condition,CustomersAttribute.customerID));
			keyAttributes.add(CustomersAttribute.customerID);
		}
		else{
			valueCond=null;
			refilterFlag.setValue(true);
		}
		if(valueCond==null)
			keypattern=keypattern.concat("*");
		else
			keypattern=keypattern.concat(valueCond);
		fieldsListInKey.add("customerid");
		keypatternAllVariables=keypatternAllVariables.concat("*");
		keypattern=keypattern.concat(":ORDERS");
		keypatternAllVariables=keypatternAllVariables.concat(":ORDERS");
		if(!refilterFlag.booleanValue()){
			Set<CustomersAttribute> conditionAttributes = Util.getConditionAttributes(condition);
			for (CustomersAttribute a : conditionAttributes) {
				if (!keyAttributes.contains(a)) {
					refilterFlag.setValue(true);
					break;
				}
			}
		}
	
			
		// Find the type of query to perform in order to retrieve a Dataset<Row>
		// Based on the type of the value. Is a it a simple string or a hash or a list... 
		Dataset<Row> rows;
		StructType structType = new StructType(new StructField[] {
			DataTypes.createStructField("_id", DataTypes.StringType, true), //technical field to store the key.
			DataTypes.createStructField("orderref", DataTypes.createArrayType(DataTypes.StringType), true)
		});
		rows = SparkConnectionMgr.getRowsFromKeyValueList("myRedisDB",keypattern, structType);
		if(rows == null || rows.isEmpty())
				return null;
		boolean isStriped = false;
		String prefix=isStriped?keypattern.substring(0, keypattern.length() - 1):"";
		finalKeypattern = keypatternAllVariables;
		Dataset<CustomersTDO> res = rows.map((MapFunction<Row, CustomersTDO>) r -> {
					CustomersTDO customers_res = new CustomersTDO();
					Integer groupindex = null;
					String regex = null;
					String value = null;
					Pattern p, pattern = null;
					Matcher m, match = null;
					boolean matches = false;
					String key = isStriped ? prefix + r.getAs("_id") : r.getAs("_id");
					// Spark Redis automatically strips leading character if the pattern provided contains a single '*' at the end.				
					pattern = Pattern.compile("\\*");
			        match = pattern.matcher(finalKeypattern);
					regex = finalKeypattern.replaceAll("\\*","(.*)");
					p = Pattern.compile(regex);
					m = p.matcher(key);
					matches = m.find();
					// attribute [Customers.CustomerID]
					// Attribute mapped in a key.
					groupindex = fieldsListInKey.indexOf("customerid")+1;
					if(groupindex==null) {
						logger.warn("Attribute 'Customers' mapped physical field 'customerid' found in key but can't get index in build keypattern '{}'.", finalKeypattern);
					}
					String customerID = null;
					if(matches) {
						customerID = m.group(groupindex.intValue());
					} else {
						logger.warn("Cannot retrieve value for CustomerscustomerID attribute stored in db myRedisDB. Regex [{}] Value [{}]",regex,value);
						customers_res.addLogEvent("Cannot retrieve value for Customers.customerID attribute stored in db myRedisDB. Probably due to an ambiguous regex.");
					}
					customers_res.setCustomerID(customerID == null ? null : customerID);
					// Get reference column in value [orderref ] for reference [purchases]
					customers_res.setKvDB_CustomersPurchased_purchases_orderref(new ArrayList<String>(ScalaUtil.javaList(r.getAs("orderref"))));
	
						return customers_res;
				}, Encoders.bean(CustomersTDO.class));
		res=res.dropDuplicates(new String[] {"customerID"});
		return res;
	}
	
	// Right side 'OrderID' of reference [purchases ]
	public Dataset<OrdersTDO> getOrdersTDOListBoughtOrderInPurchasesInCustomersPurchasedFromKvDB(Condition<OrdersAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = OrdersServiceImpl.getBSONMatchQueryInOrdersFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Orders", bsonQuery);
	
		Dataset<OrdersTDO> res = dataset.flatMap((FlatMapFunction<Row, OrdersTDO>) r -> {
				Set<OrdersTDO> list_res = new HashSet<OrdersTDO>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				OrdersTDO orders1 = new OrdersTDO();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Orders.id for field OrderID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderID")) {
						if(nestedRow.getAs("OrderID") == null){
							orders1.setId(null);
						}else{
							orders1.setId(Util.getIntegerValue(nestedRow.getAs("OrderID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.orderDate for field OrderDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderDate")) {
						if(nestedRow.getAs("OrderDate") == null){
							orders1.setOrderDate(null);
						}else{
							orders1.setOrderDate(Util.getLocalDateValue(nestedRow.getAs("OrderDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.requiredDate for field RequiredDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("RequiredDate")) {
						if(nestedRow.getAs("RequiredDate") == null){
							orders1.setRequiredDate(null);
						}else{
							orders1.setRequiredDate(Util.getLocalDateValue(nestedRow.getAs("RequiredDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.freight for field Freight			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Freight")) {
						if(nestedRow.getAs("Freight") == null){
							orders1.setFreight(null);
						}else{
							orders1.setFreight(Util.getDoubleValue(nestedRow.getAs("Freight")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shippedDate for field ShippedDate			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShippedDate")) {
						if(nestedRow.getAs("ShippedDate") == null){
							orders1.setShippedDate(null);
						}else{
							orders1.setShippedDate(Util.getLocalDateValue(nestedRow.getAs("ShippedDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipName for field ShipName			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipName")) {
						if(nestedRow.getAs("ShipName") == null){
							orders1.setShipName(null);
						}else{
							orders1.setShipName(Util.getStringValue(nestedRow.getAs("ShipName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipAddress for field ShipAddress			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipAddress")) {
						if(nestedRow.getAs("ShipAddress") == null){
							orders1.setShipAddress(null);
						}else{
							orders1.setShipAddress(Util.getStringValue(nestedRow.getAs("ShipAddress")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipCity for field ShipCity			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCity")) {
						if(nestedRow.getAs("ShipCity") == null){
							orders1.setShipCity(null);
						}else{
							orders1.setShipCity(Util.getStringValue(nestedRow.getAs("ShipCity")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipRegion for field ShipRegion			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipRegion")) {
						if(nestedRow.getAs("ShipRegion") == null){
							orders1.setShipRegion(null);
						}else{
							orders1.setShipRegion(Util.getStringValue(nestedRow.getAs("ShipRegion")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipPostalCode for field ShipPostalCode			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipPostalCode")) {
						if(nestedRow.getAs("ShipPostalCode") == null){
							orders1.setShipPostalCode(null);
						}else{
							orders1.setShipPostalCode(Util.getStringValue(nestedRow.getAs("ShipPostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.shipCountry for field ShipCountry			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCountry")) {
						if(nestedRow.getAs("ShipCountry") == null){
							orders1.setShipCountry(null);
						}else{
							orders1.setShipCountry(Util.getStringValue(nestedRow.getAs("ShipCountry")));
							toAdd1 = true;					
							}
					}
					
						// field  OrderID for reference purchases . Reference field : OrderID
					nestedRow =  r1;
					if(nestedRow != null) {
						orders1.setKvDB_CustomersPurchased_purchases_OrderID(nestedRow.getAs("OrderID") == null ? null : nestedRow.getAs("OrderID").toString());
						toAdd1 = true;					
					}
					
					
					if(toAdd1) {
						list_res.add(orders1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(OrdersTDO.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
	}
	
	
	
	
	public Dataset<Buy> getBuyList(
		Condition<OrdersAttribute> boughtOrder_condition,
		Condition<CustomersAttribute> customer_condition){
			BuyServiceImpl buyService = this;
			OrdersService ordersService = new OrdersServiceImpl();  
			CustomersService customersService = new CustomersServiceImpl();
			MutableBoolean boughtOrder_refilter = new MutableBoolean(false);
			List<Dataset<Buy>> datasetsPOJO = new ArrayList<Dataset<Buy>>();
			boolean all_already_persisted = false;
			MutableBoolean customer_refilter = new MutableBoolean(false);
			
			org.apache.spark.sql.Column joinCondition = null;
			// For role 'boughtOrder' in reference 'bought'. A->B Scenario
			customer_refilter = new MutableBoolean(false);
			Dataset<OrdersTDO> ordersTDOboughtboughtOrder = buyService.getOrdersTDOListBoughtOrderInBoughtInOrdersFromMongoDB(boughtOrder_condition, boughtOrder_refilter);
			Dataset<CustomersTDO> customersTDOboughtcustomer = buyService.getCustomersTDOListCustomerInBoughtInOrdersFromMongoDB(customer_condition, customer_refilter);
			
			Dataset<Row> res_bought_temp = ordersTDOboughtboughtOrder.join(customersTDOboughtcustomer
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
		
			Dataset<Buy> res_bought = res_bought_temp.map(
				(MapFunction<Row, Buy>) r -> {
					Buy res = new Buy();
					Orders A = new Orders();
					Customers B = new Customers();
					A.setId(Util.getIntegerValue(r.getAs("id")));
					A.setOrderDate(Util.getLocalDateValue(r.getAs("orderDate")));
					A.setRequiredDate(Util.getLocalDateValue(r.getAs("requiredDate")));
					A.setShippedDate(Util.getLocalDateValue(r.getAs("shippedDate")));
					A.setFreight(Util.getDoubleValue(r.getAs("freight")));
					A.setShipName(Util.getStringValue(r.getAs("shipName")));
					A.setShipAddress(Util.getStringValue(r.getAs("shipAddress")));
					A.setShipCity(Util.getStringValue(r.getAs("shipCity")));
					A.setShipRegion(Util.getStringValue(r.getAs("shipRegion")));
					A.setShipPostalCode(Util.getStringValue(r.getAs("shipPostalCode")));
					A.setShipCountry(Util.getStringValue(r.getAs("shipCountry")));
					A.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("logEvents")));
		
					B.setCustomerID(Util.getStringValue(r.getAs("Customers_customerID")));
					B.setCompanyName(Util.getStringValue(r.getAs("Customers_companyName")));
					B.setContactName(Util.getStringValue(r.getAs("Customers_contactName")));
					B.setContactTitle(Util.getStringValue(r.getAs("Customers_contactTitle")));
					B.setAddress(Util.getStringValue(r.getAs("Customers_address")));
					B.setCity(Util.getStringValue(r.getAs("Customers_city")));
					B.setRegion(Util.getStringValue(r.getAs("Customers_region")));
					B.setPostalCode(Util.getStringValue(r.getAs("Customers_postalCode")));
					B.setCountry(Util.getStringValue(r.getAs("Customers_country")));
					B.setPhone(Util.getStringValue(r.getAs("Customers_phone")));
					B.setFax(Util.getStringValue(r.getAs("Customers_fax")));
					B.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("Customers_logEvents")));
						
					res.setBoughtOrder(A);
					res.setCustomer(B);
					return res;
				},Encoders.bean(Buy.class)
			);
		
			datasetsPOJO.add(res_bought);
		
			customer_refilter = new MutableBoolean(false);
			// For role 'customer' in reference 'purchases'  B->A Scenario
			Dataset<CustomersTDO> customersTDOpurchasescustomer = buyService.getCustomersTDOListCustomerInPurchasesInCustomersPurchasedFromKvDB(customer_condition, customer_refilter);
			Dataset<OrdersTDO> ordersTDOpurchasesboughtOrder = buyService.getOrdersTDOListBoughtOrderInPurchasesInCustomersPurchasedFromKvDB(boughtOrder_condition, boughtOrder_refilter);
			
			// Multi valued reference	
			Dataset<Row> res_purchases_temp = 
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
		
			Dataset<Buy> res_purchases = res_purchases_temp.map(
				(MapFunction<Row, Buy>) r -> {
					Buy res = new Buy();
					Orders A = new Orders();
					Customers B = new Customers();
					A.setId(Util.getIntegerValue(r.getAs("id")));
					A.setOrderDate(Util.getLocalDateValue(r.getAs("orderDate")));
					A.setRequiredDate(Util.getLocalDateValue(r.getAs("requiredDate")));
					A.setShippedDate(Util.getLocalDateValue(r.getAs("shippedDate")));
					A.setFreight(Util.getDoubleValue(r.getAs("freight")));
					A.setShipName(Util.getStringValue(r.getAs("shipName")));
					A.setShipAddress(Util.getStringValue(r.getAs("shipAddress")));
					A.setShipCity(Util.getStringValue(r.getAs("shipCity")));
					A.setShipRegion(Util.getStringValue(r.getAs("shipRegion")));
					A.setShipPostalCode(Util.getStringValue(r.getAs("shipPostalCode")));
					A.setShipCountry(Util.getStringValue(r.getAs("shipCountry")));
					A.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("logEvents")));
		
					B.setCustomerID(Util.getStringValue(r.getAs("Customers_customerID")));
					B.setCompanyName(Util.getStringValue(r.getAs("Customers_companyName")));
					B.setContactName(Util.getStringValue(r.getAs("Customers_contactName")));
					B.setContactTitle(Util.getStringValue(r.getAs("Customers_contactTitle")));
					B.setAddress(Util.getStringValue(r.getAs("Customers_address")));
					B.setCity(Util.getStringValue(r.getAs("Customers_city")));
					B.setRegion(Util.getStringValue(r.getAs("Customers_region")));
					B.setPostalCode(Util.getStringValue(r.getAs("Customers_postalCode")));
					B.setCountry(Util.getStringValue(r.getAs("Customers_country")));
					B.setPhone(Util.getStringValue(r.getAs("Customers_phone")));
					B.setFax(Util.getStringValue(r.getAs("Customers_fax")));
					B.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("Customers_logEvents")));
						
					res.setBoughtOrder(A);
					res.setCustomer(B);
					return res;
				},Encoders.bean(Buy.class)
			);
		
					
			datasetsPOJO.add(res_purchases);
			
			Dataset<Buy> res_buy_boughtOrder;
			Dataset<Orders> res_Orders;
			
			
			//Join datasets or return 
			Dataset<Buy> res = fullOuterJoinsBuy(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Orders> lonelyBoughtOrder = null;
			Dataset<Customers> lonelyCustomer = null;
			
		
		
			
			if(boughtOrder_refilter.booleanValue() || customer_refilter.booleanValue())
				res = res.filter((FilterFunction<Buy>) r -> (boughtOrder_condition == null || boughtOrder_condition.evaluate(r.getBoughtOrder())) && (customer_condition == null || customer_condition.evaluate(r.getCustomer())));
			
		
			return res;
		
		}
	
	public Dataset<Buy> getBuyListByBoughtOrderCondition(
		Condition<OrdersAttribute> boughtOrder_condition
	){
		return getBuyList(boughtOrder_condition, null);
	}
	
	public Buy getBuyByBoughtOrder(Orders boughtOrder) {
		Condition<OrdersAttribute> cond = null;
		cond = Condition.simple(OrdersAttribute.id, Operator.EQUALS, boughtOrder.getId());
		Dataset<Buy> res = getBuyListByBoughtOrderCondition(cond);
		List<Buy> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	public Dataset<Buy> getBuyListByCustomerCondition(
		Condition<CustomersAttribute> customer_condition
	){
		return getBuyList(null, customer_condition);
	}
	
	public Dataset<Buy> getBuyListByCustomer(Customers customer) {
		Condition<CustomersAttribute> cond = null;
		cond = Condition.simple(CustomersAttribute.customerID, Operator.EQUALS, customer.getCustomerID());
		Dataset<Buy> res = getBuyListByCustomerCondition(cond);
	return res;
	}
	
	
	
	public void deleteBuyList(
		conditions.Condition<conditions.OrdersAttribute> boughtOrder_condition,
		conditions.Condition<conditions.CustomersAttribute> customer_condition){
			//TODO
		}
	
	public void deleteBuyListByBoughtOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> boughtOrder_condition
	){
		deleteBuyList(boughtOrder_condition, null);
	}
	
	public void deleteBuyByBoughtOrder(pojo.Orders boughtOrder) {
		// TODO using id for selecting
		return;
	}
	public void deleteBuyListByCustomerCondition(
		conditions.Condition<conditions.CustomersAttribute> customer_condition
	){
		deleteBuyList(null, customer_condition);
	}
	
	public void deleteBuyListByCustomer(pojo.Customers customer) {
		// TODO using id for selecting
		return;
	}
		
}
