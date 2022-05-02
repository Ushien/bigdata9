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
import conditions.SupplyAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.ProductsTDO;
import tdo.SupplyTDO;
import conditions.ProductsAttribute;
import dao.services.ProductsService;
import tdo.SuppliersTDO;
import tdo.SupplyTDO;
import conditions.SuppliersAttribute;
import dao.services.SuppliersService;
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

public class SupplyServiceImpl extends dao.services.SupplyService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SupplyServiceImpl.class);
	
	
	// Left side 'SupplierRef' of reference [supply ]
	public Dataset<ProductsTDO> getProductsTDOListSuppliedProductInSupplyInProductsFromRelDB(Condition<ProductsAttribute> condition, MutableBoolean refilterFlag){	
	
		Pair<String, List<String>> whereClause = ProductsServiceImpl.getSQLWhereClauseInProductsFromMyRelDB(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", preparedValue);
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("myRelDB", "Products", where);
		
	
		Dataset<ProductsTDO> res = d.map((MapFunction<Row, ProductsTDO>) r -> {
					ProductsTDO products_res = new ProductsTDO();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
					// attribute [Products.ProductId]
					Integer productId = Util.getIntegerValue(r.getAs("ProductID"));
					products_res.setProductId(productId);
					
					// attribute [Products.ProductName]
					String productName = Util.getStringValue(r.getAs("ProductName"));
					products_res.setProductName(productName);
					
					// attribute [Products.QuantityPerUnit]
					String quantityPerUnit = Util.getStringValue(r.getAs("QuantityPerUnit"));
					products_res.setQuantityPerUnit(quantityPerUnit);
					
					// attribute [Products.UnitPrice]
					Double unitPrice = Util.getDoubleValue(r.getAs("UnitPrice"));
					products_res.setUnitPrice(unitPrice);
					
					// attribute [Products.UnitsInStock]
					Integer unitsInStock = Util.getIntegerValue(r.getAs("UnitsInStock"));
					products_res.setUnitsInStock(unitsInStock);
					
					// attribute [Products.UnitsOnOrder]
					Integer unitsOnOrder = Util.getIntegerValue(r.getAs("UnitsOnOrder"));
					products_res.setUnitsOnOrder(unitsOnOrder);
					
					// attribute [Products.ReorderLevel]
					Integer reorderLevel = Util.getIntegerValue(r.getAs("ReorderLevel"));
					products_res.setReorderLevel(reorderLevel);
					
					// attribute [Products.Discontinued]
					Boolean discontinued = Util.getBooleanValue(r.getAs("Discontinued"));
					products_res.setDiscontinued(discontinued);
	
					// Get reference column [SupplierRef ] for reference [supply]
					String relDB_Products_supply_SupplierRef = r.getAs("SupplierRef") == null ? null : r.getAs("SupplierRef").toString();
					products_res.setRelDB_Products_supply_SupplierRef(relDB_Products_supply_SupplierRef);
	
	
					return products_res;
				}, Encoders.bean(ProductsTDO.class));
	
	
		return res;
	}
	
	// Right side 'SupplierID' of reference [supply ]
	public Dataset<SuppliersTDO> getSuppliersTDOListSupplierInSupplyInProductsFromRelDB(Condition<SuppliersAttribute> condition, MutableBoolean refilterFlag){
		// Build the key pattern
		//  - If the condition attribute is in the key pattern, replace by the value. Only if operator is EQUALS.
		//  - Replace all other fields of key pattern by a '*' 
		String keypattern= "", keypatternAllVariables="";
		String valueCond=null;
		String finalKeypattern;
		List<String> fieldsListInKey = new ArrayList<>();
		Set<SuppliersAttribute> keyAttributes = new HashSet<>();
		keypattern=keypattern.concat("SUPPLIERS:");
		keypatternAllVariables=keypatternAllVariables.concat("SUPPLIERS:");
		if(!Util.containsOrCondition(condition)){
			valueCond=Util.getStringValue(Util.getValueOfAttributeInEqualCondition(condition,SuppliersAttribute.supplierId));
			keyAttributes.add(SuppliersAttribute.supplierId);
		}
		else{
			valueCond=null;
			refilterFlag.setValue(true);
		}
		if(valueCond==null)
			keypattern=keypattern.concat("*");
		else
			keypattern=keypattern.concat(valueCond);
		fieldsListInKey.add("SupplierID");
		keypatternAllVariables=keypatternAllVariables.concat("*");
		if(!refilterFlag.booleanValue()){
			Set<SuppliersAttribute> conditionAttributes = Util.getConditionAttributes(condition);
			for (SuppliersAttribute a : conditionAttributes) {
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
	,		DataTypes.createStructField("HomePage", DataTypes.StringType, true)
		});
		rows = SparkConnectionMgr.getRowsFromKeyValueHashes("myRedisDB",keypattern, structType);
		if(rows == null || rows.isEmpty())
				return null;
		boolean isStriped = false;
		String prefix=isStriped?keypattern.substring(0, keypattern.length() - 1):"";
		finalKeypattern = keypatternAllVariables;
		Dataset<SuppliersTDO> res = rows.map((MapFunction<Row, SuppliersTDO>) r -> {
					SuppliersTDO suppliers_res = new SuppliersTDO();
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
					// attribute [Suppliers.SupplierId]
					// Attribute mapped in a key.
					groupindex = fieldsListInKey.indexOf("SupplierID")+1;
					if(groupindex==null) {
						logger.warn("Attribute 'Suppliers' mapped physical field 'SupplierID' found in key but can't get index in build keypattern '{}'.", finalKeypattern);
					}
					String supplierId = null;
					if(matches) {
						supplierId = m.group(groupindex.intValue());
					} else {
						logger.warn("Cannot retrieve value for SupplierssupplierId attribute stored in db myRedisDB. Regex [{}] Value [{}]",regex,value);
						suppliers_res.addLogEvent("Cannot retrieve value for Suppliers.supplierId attribute stored in db myRedisDB. Probably due to an ambiguous regex.");
					}
					suppliers_res.setSupplierId(supplierId == null ? null : Integer.parseInt(supplierId));
					// attribute [Suppliers.CompanyName]
					String companyName = r.getAs("CompanyName") == null ? null : r.getAs("CompanyName");
					suppliers_res.setCompanyName(companyName);
					// attribute [Suppliers.ContactName]
					String contactName = r.getAs("ContactName") == null ? null : r.getAs("ContactName");
					suppliers_res.setContactName(contactName);
					// attribute [Suppliers.ContactTitle]
					String contactTitle = r.getAs("ContactTitle") == null ? null : r.getAs("ContactTitle");
					suppliers_res.setContactTitle(contactTitle);
					// attribute [Suppliers.Address]
					String address = r.getAs("Address") == null ? null : r.getAs("Address");
					suppliers_res.setAddress(address);
					// attribute [Suppliers.City]
					String city = r.getAs("City") == null ? null : r.getAs("City");
					suppliers_res.setCity(city);
					// attribute [Suppliers.Region]
					String region = r.getAs("Region") == null ? null : r.getAs("Region");
					suppliers_res.setRegion(region);
					// attribute [Suppliers.PostalCode]
					String postalCode = r.getAs("PostalCode") == null ? null : r.getAs("PostalCode");
					suppliers_res.setPostalCode(postalCode);
					// attribute [Suppliers.Country]
					String country = r.getAs("Country") == null ? null : r.getAs("Country");
					suppliers_res.setCountry(country);
					// attribute [Suppliers.Phone]
					String phone = r.getAs("Phone") == null ? null : r.getAs("Phone");
					suppliers_res.setPhone(phone);
					// attribute [Suppliers.Fax]
					String fax = r.getAs("Fax") == null ? null : r.getAs("Fax");
					suppliers_res.setFax(fax);
					// attribute [Suppliers.HomePage]
					String homePage = r.getAs("HomePage") == null ? null : r.getAs("HomePage");
					suppliers_res.setHomePage(homePage);
					//Checking that reference field 'SupplierID' is mapped in Key
					if(fieldsListInKey.contains("SupplierID")){
						//Retrieving reference field 'SupplierID' in Key
						Pattern pattern_SupplierID = Pattern.compile("\\*");
				        Matcher match_SupplierID = pattern_SupplierID.matcher(finalKeypattern);
						regex = finalKeypattern.replaceAll("\\*","(.*)");
						groupindex = fieldsListInKey.indexOf("SupplierID")+1;
						if(groupindex==null) {
							logger.warn("Attribute 'Suppliers' mapped physical field 'SupplierID' found in key but can't get index in build keypattern '{}'.", finalKeypattern);
						}
						p = Pattern.compile(regex);
						m = p.matcher(key);
						matches = m.find();
						String supply_SupplierID = null;
						if(matches) {
						supply_SupplierID = m.group(groupindex.intValue());
						} else {
						logger.warn("Cannot retrieve value 'SupplierID'. Regex [{}] Value [{}]",regex,value);
						suppliers_res.addLogEvent("Cannot retrieve value for 'SupplierID' attribute stored in db myRedisDB. Probably due to an ambiguous regex.");
						}
						suppliers_res.setRelDB_Products_supply_SupplierID(supply_SupplierID);
					}
	
						return suppliers_res;
				}, Encoders.bean(SuppliersTDO.class));
		res=res.dropDuplicates(new String[] {"supplierId"});
		return res;
	}
	
	
	
	
	public Dataset<Supply> getSupplyList(
		Condition<ProductsAttribute> suppliedProduct_condition,
		Condition<SuppliersAttribute> supplier_condition){
			SupplyServiceImpl supplyService = this;
			ProductsService productsService = new ProductsServiceImpl();  
			SuppliersService suppliersService = new SuppliersServiceImpl();
			MutableBoolean suppliedProduct_refilter = new MutableBoolean(false);
			List<Dataset<Supply>> datasetsPOJO = new ArrayList<Dataset<Supply>>();
			boolean all_already_persisted = false;
			MutableBoolean supplier_refilter = new MutableBoolean(false);
			
			org.apache.spark.sql.Column joinCondition = null;
			// For role 'suppliedProduct' in reference 'supply'. A->B Scenario
			supplier_refilter = new MutableBoolean(false);
			Dataset<ProductsTDO> productsTDOsupplysuppliedProduct = supplyService.getProductsTDOListSuppliedProductInSupplyInProductsFromRelDB(suppliedProduct_condition, suppliedProduct_refilter);
			Dataset<SuppliersTDO> suppliersTDOsupplysupplier = supplyService.getSuppliersTDOListSupplierInSupplyInProductsFromRelDB(supplier_condition, supplier_refilter);
			
			Dataset<Row> res_supply_temp = productsTDOsupplysuppliedProduct.join(suppliersTDOsupplysupplier
					.withColumnRenamed("supplierId", "Suppliers_supplierId")
					.withColumnRenamed("companyName", "Suppliers_companyName")
					.withColumnRenamed("contactName", "Suppliers_contactName")
					.withColumnRenamed("contactTitle", "Suppliers_contactTitle")
					.withColumnRenamed("address", "Suppliers_address")
					.withColumnRenamed("city", "Suppliers_city")
					.withColumnRenamed("region", "Suppliers_region")
					.withColumnRenamed("postalCode", "Suppliers_postalCode")
					.withColumnRenamed("country", "Suppliers_country")
					.withColumnRenamed("phone", "Suppliers_phone")
					.withColumnRenamed("fax", "Suppliers_fax")
					.withColumnRenamed("homePage", "Suppliers_homePage")
					.withColumnRenamed("logEvents", "Suppliers_logEvents"),
					productsTDOsupplysuppliedProduct.col("relDB_Products_supply_SupplierRef").equalTo(suppliersTDOsupplysupplier.col("relDB_Products_supply_SupplierID")));
		
			Dataset<Supply> res_supply = res_supply_temp.map(
				(MapFunction<Row, Supply>) r -> {
					Supply res = new Supply();
					Products A = new Products();
					Suppliers B = new Suppliers();
					A.setProductId(Util.getIntegerValue(r.getAs("productId")));
					A.setProductName(Util.getStringValue(r.getAs("productName")));
					A.setQuantityPerUnit(Util.getStringValue(r.getAs("quantityPerUnit")));
					A.setUnitPrice(Util.getDoubleValue(r.getAs("unitPrice")));
					A.setUnitsInStock(Util.getIntegerValue(r.getAs("unitsInStock")));
					A.setUnitsOnOrder(Util.getIntegerValue(r.getAs("unitsOnOrder")));
					A.setReorderLevel(Util.getIntegerValue(r.getAs("reorderLevel")));
					A.setDiscontinued(Util.getBooleanValue(r.getAs("discontinued")));
					A.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("logEvents")));
		
					B.setSupplierId(Util.getIntegerValue(r.getAs("Suppliers_supplierId")));
					B.setCompanyName(Util.getStringValue(r.getAs("Suppliers_companyName")));
					B.setContactName(Util.getStringValue(r.getAs("Suppliers_contactName")));
					B.setContactTitle(Util.getStringValue(r.getAs("Suppliers_contactTitle")));
					B.setAddress(Util.getStringValue(r.getAs("Suppliers_address")));
					B.setCity(Util.getStringValue(r.getAs("Suppliers_city")));
					B.setRegion(Util.getStringValue(r.getAs("Suppliers_region")));
					B.setPostalCode(Util.getStringValue(r.getAs("Suppliers_postalCode")));
					B.setCountry(Util.getStringValue(r.getAs("Suppliers_country")));
					B.setPhone(Util.getStringValue(r.getAs("Suppliers_phone")));
					B.setFax(Util.getStringValue(r.getAs("Suppliers_fax")));
					B.setHomePage(Util.getStringValue(r.getAs("Suppliers_homePage")));
					B.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("Suppliers_logEvents")));
						
					res.setSuppliedProduct(A);
					res.setSupplier(B);
					return res;
				},Encoders.bean(Supply.class)
			);
		
			datasetsPOJO.add(res_supply);
		
			
			Dataset<Supply> res_supply_suppliedProduct;
			Dataset<Products> res_Products;
			
			
			//Join datasets or return 
			Dataset<Supply> res = fullOuterJoinsSupply(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Products> lonelySuppliedProduct = null;
			Dataset<Suppliers> lonelySupplier = null;
			
		
		
			
			if(suppliedProduct_refilter.booleanValue() || supplier_refilter.booleanValue())
				res = res.filter((FilterFunction<Supply>) r -> (suppliedProduct_condition == null || suppliedProduct_condition.evaluate(r.getSuppliedProduct())) && (supplier_condition == null || supplier_condition.evaluate(r.getSupplier())));
			
		
			return res;
		
		}
	
	public Dataset<Supply> getSupplyListBySuppliedProductCondition(
		Condition<ProductsAttribute> suppliedProduct_condition
	){
		return getSupplyList(suppliedProduct_condition, null);
	}
	
	public Supply getSupplyBySuppliedProduct(Products suppliedProduct) {
		Condition<ProductsAttribute> cond = null;
		cond = Condition.simple(ProductsAttribute.productId, Operator.EQUALS, suppliedProduct.getProductId());
		Dataset<Supply> res = getSupplyListBySuppliedProductCondition(cond);
		List<Supply> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	public Dataset<Supply> getSupplyListBySupplierCondition(
		Condition<SuppliersAttribute> supplier_condition
	){
		return getSupplyList(null, supplier_condition);
	}
	
	public Dataset<Supply> getSupplyListBySupplier(Suppliers supplier) {
		Condition<SuppliersAttribute> cond = null;
		cond = Condition.simple(SuppliersAttribute.supplierId, Operator.EQUALS, supplier.getSupplierId());
		Dataset<Supply> res = getSupplyListBySupplierCondition(cond);
	return res;
	}
	
	public void insertSupply(Supply supply){
		//Link entities in join structures.
		// Update embedded structures mapped to non mandatory roles.
		// Update ref fields mapped to non mandatory roles. 
		insertSupplyInRefStructProductsInMyRelDB(supply);
	}
	
	
	
	public 	boolean insertSupplyInRefStructProductsInMyRelDB(Supply supply){
	 	// Rel 'supply' Insert in reference structure 'Products'
		Products productsSuppliedProduct = supply.getSuppliedProduct();
		Suppliers suppliersSupplier = supply.getSupplier();
	
		List<String> columns = new ArrayList<>();
		List<Object> values = new ArrayList<>();
		String filtercolumn;
		Object filtervalue;
		columns.add("SupplierRef");
		values.add(suppliersSupplier==null?null:suppliersSupplier.getSupplierId());
		filtercolumn = "ProductID";
		filtervalue = productsSuppliedProduct.getProductId();
		DBConnectionMgr.updateInTable(filtercolumn, filtervalue, columns, values, "Products", "myRelDB");					
		return true;
	}
	
	
	
	
	public void deleteSupplyList(
		conditions.Condition<conditions.ProductsAttribute> suppliedProduct_condition,
		conditions.Condition<conditions.SuppliersAttribute> supplier_condition){
			//TODO
		}
	
	public void deleteSupplyListBySuppliedProductCondition(
		conditions.Condition<conditions.ProductsAttribute> suppliedProduct_condition
	){
		deleteSupplyList(suppliedProduct_condition, null);
	}
	
	public void deleteSupplyBySuppliedProduct(pojo.Products suppliedProduct) {
		// TODO using id for selecting
		return;
	}
	public void deleteSupplyListBySupplierCondition(
		conditions.Condition<conditions.SuppliersAttribute> supplier_condition
	){
		deleteSupplyList(null, supplier_condition);
	}
	
	public void deleteSupplyListBySupplier(pojo.Suppliers supplier) {
		// TODO using id for selecting
		return;
	}
		
}
