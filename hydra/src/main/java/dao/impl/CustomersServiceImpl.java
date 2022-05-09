package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Customers;
import conditions.*;
import dao.services.CustomersService;
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


public class CustomersServiceImpl extends CustomersService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CustomersServiceImpl.class);
	
	
	
	
	
	
	
	//TODO redis
	public Dataset<Customers> getCustomersListInCustomersFromMyRedisDB(conditions.Condition<conditions.CustomersAttribute> condition, MutableBoolean refilterFlag){
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
		Dataset<Customers> res = rows.map((MapFunction<Row, Customers>) r -> {
					Customers customers_res = new Customers();
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
	
						return customers_res;
				}, Encoders.bean(Customers.class));
		res=res.dropDuplicates(new String[] {"customerID"});
		return res;
		
	}
	
	
	
	
	//TODO redis
	public Dataset<Customers> getCustomersListInCustomersPurchasedFromMyRedisDB(conditions.Condition<conditions.CustomersAttribute> condition, MutableBoolean refilterFlag){
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
		Dataset<Customers> res = rows.map((MapFunction<Row, Customers>) r -> {
					Customers customers_res = new Customers();
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
	
						return customers_res;
				}, Encoders.bean(Customers.class));
		res=res.dropDuplicates(new String[] {"customerID"});
		return res;
		
	}
	
	
	
	
	
	
	public Dataset<Customers> getCustomerListInBuy(conditions.Condition<conditions.OrdersAttribute> boughtOrder_condition,conditions.Condition<conditions.CustomersAttribute> customer_condition)		{
		MutableBoolean customer_refilter = new MutableBoolean(false);
		List<Dataset<Customers>> datasetsPOJO = new ArrayList<Dataset<Customers>>();
		Dataset<Orders> all = null;
		boolean all_already_persisted = false;
		MutableBoolean boughtOrder_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		// For role 'customer' in reference 'purchases'. A->B Scenario
		boughtOrder_refilter = new MutableBoolean(false);
		Dataset<CustomersTDO> customersTDOpurchasescustomer = buyService.getCustomersTDOListCustomerInPurchasesInCustomersPurchasedFromKvDB(customer_condition, customer_refilter);
		Dataset<OrdersTDO> ordersTDOpurchasesboughtOrder = buyService.getOrdersTDOListBoughtOrderInPurchasesInCustomersPurchasedFromKvDB(boughtOrder_condition, boughtOrder_refilter);
		if(boughtOrder_refilter.booleanValue()) {
			if(all == null)
				all = new OrdersServiceImpl().getOrdersList(boughtOrder_condition);
			joinCondition = null;
			joinCondition = ordersTDOpurchasesboughtOrder.col("id").equalTo(all.col("id"));
			if(joinCondition == null)
				ordersTDOpurchasesboughtOrder = ordersTDOpurchasesboughtOrder.as("A").join(all).select("A.*").as(Encoders.bean(OrdersTDO.class));
			else
				ordersTDOpurchasesboughtOrder = ordersTDOpurchasesboughtOrder.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(OrdersTDO.class));
		}
	
		
		Dataset<Row> res_purchases = customersTDOpurchasescustomer.join(ordersTDOpurchasesboughtOrder
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
				// Multi valued reference
				functions.array_contains(customersTDOpurchasescustomer.col("kvDB_CustomersPurchased_purchases_orderref"),ordersTDOpurchasesboughtOrder.col("kvDB_CustomersPurchased_purchases_OrderID")));
		Dataset<Customers> res_Customers_purchases = res_purchases.select( "customerID", "companyName", "contactName", "contactTitle", "address", "city", "region", "postalCode", "country", "phone", "fax", "logEvents").as(Encoders.bean(Customers.class));
		
		res_Customers_purchases = res_Customers_purchases.dropDuplicates(new String[] {"customerID"});
		datasetsPOJO.add(res_Customers_purchases);
		
		boughtOrder_refilter = new MutableBoolean(false);
		// For role 'boughtOrder' in reference 'bought'  B->A Scenario
		Dataset<OrdersTDO> ordersTDOboughtboughtOrder = buyService.getOrdersTDOListBoughtOrderInBoughtInOrdersFromMongoDB(boughtOrder_condition, boughtOrder_refilter);
		Dataset<CustomersTDO> customersTDOboughtcustomer = buyService.getCustomersTDOListCustomerInBoughtInOrdersFromMongoDB(customer_condition, customer_refilter);
		if(boughtOrder_refilter.booleanValue()) {
			if(all == null)
				all = new OrdersServiceImpl().getOrdersList(boughtOrder_condition);
			joinCondition = null;
			joinCondition = ordersTDOboughtboughtOrder.col("id").equalTo(all.col("id"));
			if(joinCondition == null)
				ordersTDOboughtboughtOrder = ordersTDOboughtboughtOrder.as("A").join(all).select("A.*").as(Encoders.bean(OrdersTDO.class));
			else
				ordersTDOboughtboughtOrder = ordersTDOboughtboughtOrder.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(OrdersTDO.class));
		}
		Dataset<Row> res_bought = 
			customersTDOboughtcustomer.join(ordersTDOboughtboughtOrder
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
				customersTDOboughtcustomer.col("mongoDB_Orders_bought_custid").equalTo(ordersTDOboughtboughtOrder.col("mongoDB_Orders_bought_CustomerRef")));
		Dataset<Customers> res_Customers_bought = res_bought.select( "customerID", "companyName", "contactName", "contactTitle", "address", "city", "region", "postalCode", "country", "phone", "fax", "logEvents").as(Encoders.bean(Customers.class));
		res_Customers_bought = res_Customers_bought.dropDuplicates(new String[] {"customerID"});
		datasetsPOJO.add(res_Customers_bought);
		
		Dataset<Buy> res_buy_customer;
		Dataset<Customers> res_Customers;
		
		
		//Join datasets or return 
		Dataset<Customers> res = fullOuterJoinsCustomers(datasetsPOJO);
		if(res == null)
			return null;
	
		if(customer_refilter.booleanValue())
			res = res.filter((FilterFunction<Customers>) r -> customer_condition == null || customer_condition.evaluate(r));
		
	
		return res;
		}
	
	
	public boolean insertCustomers(Customers customers){
		// Insert into all mapped standalone AbstractPhysicalStructure 
		boolean inserted = false;
			inserted = insertCustomersInCustomersFromMyRedisDB(customers) || inserted ;
			inserted = insertCustomersInCustomersPurchasedFromMyRedisDB(customers) || inserted ;
		return inserted;
	}
	
	public boolean insertCustomersInCustomersFromMyRedisDB(Customers customers)	{
		String idvalue="";
		idvalue+=customers.getCustomerID();
		boolean entityExists = false; // Modify in acceleo code (in 'main.services.insert.entitytype.generateSimpleInsertMethods.mtl') to generate checking before insert
		if(!entityExists){
			String key="";
			key += "CUSTOMER:";
			key += customers.getCustomerID();
			// Generate for hash value
			boolean toAdd = false;
			List<Tuple2<String,String>> hash = new ArrayList<>();
			toAdd = false;
			String _fieldname_CompanyName="CompanyName";
			String _value_CompanyName="";
			if(customers.getCompanyName()!=null){
				toAdd = true;
				_value_CompanyName += customers.getCompanyName();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_CompanyName,_value_CompanyName));
			toAdd = false;
			String _fieldname_ContactName="ContactName";
			String _value_ContactName="";
			if(customers.getContactName()!=null){
				toAdd = true;
				_value_ContactName += customers.getContactName();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_ContactName,_value_ContactName));
			toAdd = false;
			String _fieldname_ContactTitle="ContactTitle";
			String _value_ContactTitle="";
			if(customers.getContactTitle()!=null){
				toAdd = true;
				_value_ContactTitle += customers.getContactTitle();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_ContactTitle,_value_ContactTitle));
			toAdd = false;
			String _fieldname_Address="Address";
			String _value_Address="";
			if(customers.getAddress()!=null){
				toAdd = true;
				_value_Address += customers.getAddress();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_Address,_value_Address));
			toAdd = false;
			String _fieldname_City="City";
			String _value_City="";
			if(customers.getCity()!=null){
				toAdd = true;
				_value_City += customers.getCity();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_City,_value_City));
			toAdd = false;
			String _fieldname_Region="Region";
			String _value_Region="";
			if(customers.getRegion()!=null){
				toAdd = true;
				_value_Region += customers.getRegion();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_Region,_value_Region));
			toAdd = false;
			String _fieldname_PostalCode="PostalCode";
			String _value_PostalCode="";
			if(customers.getPostalCode()!=null){
				toAdd = true;
				_value_PostalCode += customers.getPostalCode();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_PostalCode,_value_PostalCode));
			toAdd = false;
			String _fieldname_Country="Country";
			String _value_Country="";
			if(customers.getCountry()!=null){
				toAdd = true;
				_value_Country += customers.getCountry();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_Country,_value_Country));
			toAdd = false;
			String _fieldname_Phone="Phone";
			String _value_Phone="";
			if(customers.getPhone()!=null){
				toAdd = true;
				_value_Phone += customers.getPhone();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_Phone,_value_Phone));
			toAdd = false;
			String _fieldname_Fax="Fax";
			String _value_Fax="";
			if(customers.getFax()!=null){
				toAdd = true;
				_value_Fax += customers.getFax();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_Fax,_value_Fax));
			
			
			
			SparkConnectionMgr.writeKeyValueHash(key,hash, "myRedisDB");
	
			logger.info("Inserted [Customers] entity ID [{}] in [Customers] in database [MyRedisDB]", idvalue);
		}
		else
			logger.warn("[Customers] entity ID [{}] already present in [Customers] in database [MyRedisDB]", idvalue);
		return !entityExists;
	} 
	
	public boolean insertCustomersInCustomersPurchasedFromMyRedisDB(Customers customers)	{
		String idvalue="";
		idvalue+=customers.getCustomerID();
		boolean entityExists = false; // Modify in acceleo code (in 'main.services.insert.entitytype.generateSimpleInsertMethods.mtl') to generate checking before insert
		if(!entityExists){
			String key="";
			key += "CUSTOMER:";
			key += customers.getCustomerID();
			key += ":ORDERS";
			boolean toAdd = false;
			String value="";
			//No addition of key value pair when the value is null/empty.
			if(toAdd)
				SparkConnectionMgr.writeKeyValue(key,value,"myRedisDB");
	
			logger.info("Inserted [Customers] entity ID [{}] in [CustomersPurchased] in database [MyRedisDB]", idvalue);
		}
		else
			logger.warn("[Customers] entity ID [{}] already present in [CustomersPurchased] in database [MyRedisDB]", idvalue);
		return !entityExists;
	} 
	
	private boolean inUpdateMethod = false;
	private List<Row> allCustomersIdList = null;
	public void updateCustomersList(conditions.Condition<conditions.CustomersAttribute> condition, conditions.SetClause<conditions.CustomersAttribute> set){
		inUpdateMethod = true;
		try {
			MutableBoolean refilterInCustomersFromMyRedisDB = new MutableBoolean(false);
			//TODO
			// one first updates in the structures necessitating to execute a "SELECT *" query to establish the update condition 
			if(refilterInCustomersFromMyRedisDB.booleanValue())
				updateCustomersListInCustomersFromMyRedisDB(condition, set);
		
			MutableBoolean refilterInCustomersPurchasedFromMyRedisDB = new MutableBoolean(false);
			//TODO
			// one first updates in the structures necessitating to execute a "SELECT *" query to establish the update condition 
			if(refilterInCustomersPurchasedFromMyRedisDB.booleanValue())
				updateCustomersListInCustomersPurchasedFromMyRedisDB(condition, set);
		
	
			if(!refilterInCustomersFromMyRedisDB.booleanValue())
				updateCustomersListInCustomersFromMyRedisDB(condition, set);
			if(!refilterInCustomersPurchasedFromMyRedisDB.booleanValue())
				updateCustomersListInCustomersPurchasedFromMyRedisDB(condition, set);
	
		} finally {
			inUpdateMethod = false;
		}
	}
	
	
	public void updateCustomersListInCustomersFromMyRedisDB(Condition<CustomersAttribute> condition, SetClause<CustomersAttribute> set) {
		//TODO
	}
	public void updateCustomersListInCustomersPurchasedFromMyRedisDB(Condition<CustomersAttribute> condition, SetClause<CustomersAttribute> set) {
		//TODO
	}
	
	
	
	public void updateCustomers(pojo.Customers customers) {
		//TODO using the id
		return;
	}
	public void updateCustomerListInBuy(
		conditions.Condition<conditions.OrdersAttribute> boughtOrder_condition,
		conditions.Condition<conditions.CustomersAttribute> customer_condition,
		
		conditions.SetClause<conditions.CustomersAttribute> set
	){
		//TODO
	}
	
	public void updateCustomerListInBuyByBoughtOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> boughtOrder_condition,
		conditions.SetClause<conditions.CustomersAttribute> set
	){
		updateCustomerListInBuy(boughtOrder_condition, null, set);
	}
	
	public void updateCustomerInBuyByBoughtOrder(
		pojo.Orders boughtOrder,
		conditions.SetClause<conditions.CustomersAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateCustomerListInBuyByCustomerCondition(
		conditions.Condition<conditions.CustomersAttribute> customer_condition,
		conditions.SetClause<conditions.CustomersAttribute> set
	){
		updateCustomerListInBuy(null, customer_condition, set);
	}
	
	
	public void deleteCustomersList(conditions.Condition<conditions.CustomersAttribute> condition){
		//TODO
	}
	
	public void deleteCustomers(pojo.Customers customers) {
		//TODO using the id
		return;
	}
	public void deleteCustomerListInBuy(	
		conditions.Condition<conditions.OrdersAttribute> boughtOrder_condition,	
		conditions.Condition<conditions.CustomersAttribute> customer_condition){
			//TODO
		}
	
	public void deleteCustomerListInBuyByBoughtOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> boughtOrder_condition
	){
		deleteCustomerListInBuy(boughtOrder_condition, null);
	}
	
	public void deleteCustomerInBuyByBoughtOrder(
		pojo.Orders boughtOrder 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteCustomerListInBuyByCustomerCondition(
		conditions.Condition<conditions.CustomersAttribute> customer_condition
	){
		deleteCustomerListInBuy(null, customer_condition);
	}
	
}
