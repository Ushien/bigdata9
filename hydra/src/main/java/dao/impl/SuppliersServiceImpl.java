package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Suppliers;
import conditions.*;
import dao.services.SuppliersService;
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


public class SuppliersServiceImpl extends SuppliersService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SuppliersServiceImpl.class);
	
	
	
	
	
	
	
	//TODO redis
	public Dataset<Suppliers> getSuppliersListInSuppliersFromMyRedisDB(conditions.Condition<conditions.SuppliersAttribute> condition, MutableBoolean refilterFlag){
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
		Dataset<Suppliers> res = rows.map((MapFunction<Row, Suppliers>) r -> {
					Suppliers suppliers_res = new Suppliers();
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
	
						return suppliers_res;
				}, Encoders.bean(Suppliers.class));
		res=res.dropDuplicates(new String[] {"supplierId"});
		return res;
		
	}
	
	
	
	
	
	
	public Dataset<Suppliers> getSupplierListInSupply(conditions.Condition<conditions.ProductsAttribute> suppliedProduct_condition,conditions.Condition<conditions.SuppliersAttribute> supplier_condition)		{
		MutableBoolean supplier_refilter = new MutableBoolean(false);
		List<Dataset<Suppliers>> datasetsPOJO = new ArrayList<Dataset<Suppliers>>();
		Dataset<Products> all = null;
		boolean all_already_persisted = false;
		MutableBoolean suppliedProduct_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		suppliedProduct_refilter = new MutableBoolean(false);
		// For role 'suppliedProduct' in reference 'supply'  B->A Scenario
		Dataset<ProductsTDO> productsTDOsupplysuppliedProduct = supplyService.getProductsTDOListSuppliedProductInSupplyInProductsFromRelDB(suppliedProduct_condition, suppliedProduct_refilter);
		Dataset<SuppliersTDO> suppliersTDOsupplysupplier = supplyService.getSuppliersTDOListSupplierInSupplyInProductsFromRelDB(supplier_condition, supplier_refilter);
		if(suppliedProduct_refilter.booleanValue()) {
			if(all == null)
				all = new ProductsServiceImpl().getProductsList(suppliedProduct_condition);
			joinCondition = null;
			joinCondition = productsTDOsupplysuppliedProduct.col("productId").equalTo(all.col("productId"));
			if(joinCondition == null)
				productsTDOsupplysuppliedProduct = productsTDOsupplysuppliedProduct.as("A").join(all).select("A.*").as(Encoders.bean(ProductsTDO.class));
			else
				productsTDOsupplysuppliedProduct = productsTDOsupplysuppliedProduct.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(ProductsTDO.class));
		}
		Dataset<Row> res_supply = 
			suppliersTDOsupplysupplier.join(productsTDOsupplysuppliedProduct
				.withColumnRenamed("productId", "Products_productId")
				.withColumnRenamed("productName", "Products_productName")
				.withColumnRenamed("quantityPerUnit", "Products_quantityPerUnit")
				.withColumnRenamed("unitPrice", "Products_unitPrice")
				.withColumnRenamed("unitsInStock", "Products_unitsInStock")
				.withColumnRenamed("unitsOnOrder", "Products_unitsOnOrder")
				.withColumnRenamed("reorderLevel", "Products_reorderLevel")
				.withColumnRenamed("discontinued", "Products_discontinued")
				.withColumnRenamed("logEvents", "Products_logEvents"),
				suppliersTDOsupplysupplier.col("relDB_Products_supply_SupplierID").equalTo(productsTDOsupplysuppliedProduct.col("relDB_Products_supply_SupplierRef")));
		Dataset<Suppliers> res_Suppliers_supply = res_supply.select( "supplierId", "companyName", "contactName", "contactTitle", "address", "city", "region", "postalCode", "country", "phone", "fax", "homePage", "logEvents").as(Encoders.bean(Suppliers.class));
		res_Suppliers_supply = res_Suppliers_supply.dropDuplicates(new String[] {"supplierId"});
		datasetsPOJO.add(res_Suppliers_supply);
		
		Dataset<Supply> res_supply_supplier;
		Dataset<Suppliers> res_Suppliers;
		
		
		//Join datasets or return 
		Dataset<Suppliers> res = fullOuterJoinsSuppliers(datasetsPOJO);
		if(res == null)
			return null;
	
		if(supplier_refilter.booleanValue())
			res = res.filter((FilterFunction<Suppliers>) r -> supplier_condition == null || supplier_condition.evaluate(r));
		
	
		return res;
		}
	
	
	public boolean insertSuppliers(Suppliers suppliers){
		// Insert into all mapped standalone AbstractPhysicalStructure 
		boolean inserted = false;
			inserted = insertSuppliersInSuppliersFromMyRedisDB(suppliers) || inserted ;
		return inserted;
	}
	
	public boolean insertSuppliersInSuppliersFromMyRedisDB(Suppliers suppliers)	{
		String idvalue="";
		idvalue+=suppliers.getSupplierId();
		boolean entityExists = false; // Modify in acceleo code (in 'main.services.insert.entitytype.generateSimpleInsertMethods.mtl') to generate checking before insert
		if(!entityExists){
			String key="";
			key += "SUPPLIERS:";
			key += suppliers.getSupplierId();
			// Generate for hash value
			boolean toAdd = false;
			List<Tuple2<String,String>> hash = new ArrayList<>();
			toAdd = false;
			String _fieldname_CompanyName="CompanyName";
			String _value_CompanyName="";
			if(suppliers.getCompanyName()!=null){
				toAdd = true;
				_value_CompanyName += suppliers.getCompanyName();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_CompanyName,_value_CompanyName));
			toAdd = false;
			String _fieldname_ContactName="ContactName";
			String _value_ContactName="";
			if(suppliers.getContactName()!=null){
				toAdd = true;
				_value_ContactName += suppliers.getContactName();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_ContactName,_value_ContactName));
			toAdd = false;
			String _fieldname_ContactTitle="ContactTitle";
			String _value_ContactTitle="";
			if(suppliers.getContactTitle()!=null){
				toAdd = true;
				_value_ContactTitle += suppliers.getContactTitle();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_ContactTitle,_value_ContactTitle));
			toAdd = false;
			String _fieldname_Address="Address";
			String _value_Address="";
			if(suppliers.getAddress()!=null){
				toAdd = true;
				_value_Address += suppliers.getAddress();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_Address,_value_Address));
			toAdd = false;
			String _fieldname_City="City";
			String _value_City="";
			if(suppliers.getCity()!=null){
				toAdd = true;
				_value_City += suppliers.getCity();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_City,_value_City));
			toAdd = false;
			String _fieldname_Region="Region";
			String _value_Region="";
			if(suppliers.getRegion()!=null){
				toAdd = true;
				_value_Region += suppliers.getRegion();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_Region,_value_Region));
			toAdd = false;
			String _fieldname_PostalCode="PostalCode";
			String _value_PostalCode="";
			if(suppliers.getPostalCode()!=null){
				toAdd = true;
				_value_PostalCode += suppliers.getPostalCode();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_PostalCode,_value_PostalCode));
			toAdd = false;
			String _fieldname_Country="Country";
			String _value_Country="";
			if(suppliers.getCountry()!=null){
				toAdd = true;
				_value_Country += suppliers.getCountry();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_Country,_value_Country));
			toAdd = false;
			String _fieldname_Phone="Phone";
			String _value_Phone="";
			if(suppliers.getPhone()!=null){
				toAdd = true;
				_value_Phone += suppliers.getPhone();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_Phone,_value_Phone));
			toAdd = false;
			String _fieldname_Fax="Fax";
			String _value_Fax="";
			if(suppliers.getFax()!=null){
				toAdd = true;
				_value_Fax += suppliers.getFax();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_Fax,_value_Fax));
			toAdd = false;
			String _fieldname_HomePage="HomePage";
			String _value_HomePage="";
			if(suppliers.getHomePage()!=null){
				toAdd = true;
				_value_HomePage += suppliers.getHomePage();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_HomePage,_value_HomePage));
			
			
			
			SparkConnectionMgr.writeKeyValueHash(key,hash, "myRedisDB");
	
			logger.info("Inserted [Suppliers] entity ID [{}] in [Suppliers] in database [MyRedisDB]", idvalue);
		}
		else
			logger.warn("[Suppliers] entity ID [{}] already present in [Suppliers] in database [MyRedisDB]", idvalue);
		return !entityExists;
	} 
	
	private boolean inUpdateMethod = false;
	private List<Row> allSuppliersIdList = null;
	public void updateSuppliersList(conditions.Condition<conditions.SuppliersAttribute> condition, conditions.SetClause<conditions.SuppliersAttribute> set){
		inUpdateMethod = true;
		try {
			MutableBoolean refilterInSuppliersFromMyRedisDB = new MutableBoolean(false);
			//TODO
			// one first updates in the structures necessitating to execute a "SELECT *" query to establish the update condition 
			if(refilterInSuppliersFromMyRedisDB.booleanValue())
				updateSuppliersListInSuppliersFromMyRedisDB(condition, set);
		
	
			if(!refilterInSuppliersFromMyRedisDB.booleanValue())
				updateSuppliersListInSuppliersFromMyRedisDB(condition, set);
	
		} finally {
			inUpdateMethod = false;
		}
	}
	
	
	public void updateSuppliersListInSuppliersFromMyRedisDB(Condition<SuppliersAttribute> condition, SetClause<SuppliersAttribute> set) {
		//TODO
	}
	
	
	
	public void updateSuppliers(pojo.Suppliers suppliers) {
		//TODO using the id
		return;
	}
	public void updateSupplierListInSupply(
		conditions.Condition<conditions.ProductsAttribute> suppliedProduct_condition,
		conditions.Condition<conditions.SuppliersAttribute> supplier_condition,
		
		conditions.SetClause<conditions.SuppliersAttribute> set
	){
		//TODO
	}
	
	public void updateSupplierListInSupplyBySuppliedProductCondition(
		conditions.Condition<conditions.ProductsAttribute> suppliedProduct_condition,
		conditions.SetClause<conditions.SuppliersAttribute> set
	){
		updateSupplierListInSupply(suppliedProduct_condition, null, set);
	}
	
	public void updateSupplierInSupplyBySuppliedProduct(
		pojo.Products suppliedProduct,
		conditions.SetClause<conditions.SuppliersAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateSupplierListInSupplyBySupplierCondition(
		conditions.Condition<conditions.SuppliersAttribute> supplier_condition,
		conditions.SetClause<conditions.SuppliersAttribute> set
	){
		updateSupplierListInSupply(null, supplier_condition, set);
	}
	
	
	public void deleteSuppliersList(conditions.Condition<conditions.SuppliersAttribute> condition){
		//TODO
	}
	
	public void deleteSuppliers(pojo.Suppliers suppliers) {
		//TODO using the id
		return;
	}
	public void deleteSupplierListInSupply(	
		conditions.Condition<conditions.ProductsAttribute> suppliedProduct_condition,	
		conditions.Condition<conditions.SuppliersAttribute> supplier_condition){
			//TODO
		}
	
	public void deleteSupplierListInSupplyBySuppliedProductCondition(
		conditions.Condition<conditions.ProductsAttribute> suppliedProduct_condition
	){
		deleteSupplierListInSupply(suppliedProduct_condition, null);
	}
	
	public void deleteSupplierInSupplyBySuppliedProduct(
		pojo.Products suppliedProduct 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteSupplierListInSupplyBySupplierCondition(
		conditions.Condition<conditions.SuppliersAttribute> supplier_condition
	){
		deleteSupplierListInSupply(null, supplier_condition);
	}
	
}
