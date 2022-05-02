package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import conditions.*;
import pojo.Supply;
import java.time.LocalDate;
import java.time.LocalDateTime;
import tdo.*;
import pojo.*;
import org.apache.commons.lang.mutable.MutableBoolean;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.MapFunction;
import util.Util;


public abstract class SupplyService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SupplyService.class);
	
	
	// Left side 'SupplierRef' of reference [supply ]
	public abstract Dataset<ProductsTDO> getProductsTDOListSuppliedProductInSupplyInProductsFromRelDB(Condition<ProductsAttribute> condition, MutableBoolean refilterFlag);
	
	// Right side 'SupplierID' of reference [supply ]
	public abstract Dataset<SuppliersTDO> getSuppliersTDOListSupplierInSupplyInProductsFromRelDB(Condition<SuppliersAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	public static Dataset<Supply> fullLeftOuterJoinBetweenSupplyAndSuppliedProduct(Dataset<Supply> d1, Dataset<Products> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("productId", "A_productId")
			.withColumnRenamed("productName", "A_productName")
			.withColumnRenamed("quantityPerUnit", "A_quantityPerUnit")
			.withColumnRenamed("unitPrice", "A_unitPrice")
			.withColumnRenamed("unitsInStock", "A_unitsInStock")
			.withColumnRenamed("unitsOnOrder", "A_unitsOnOrder")
			.withColumnRenamed("reorderLevel", "A_reorderLevel")
			.withColumnRenamed("discontinued", "A_discontinued")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("suppliedProduct.productId").equalTo(d2_.col("A_productId"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Supply>) r -> {
				Supply res = new Supply();
	
				Products suppliedProduct = new Products();
				Object o = r.getAs("suppliedProduct");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						suppliedProduct.setProductId(Util.getIntegerValue(r2.getAs("productId")));
						suppliedProduct.setProductName(Util.getStringValue(r2.getAs("productName")));
						suppliedProduct.setQuantityPerUnit(Util.getStringValue(r2.getAs("quantityPerUnit")));
						suppliedProduct.setUnitPrice(Util.getDoubleValue(r2.getAs("unitPrice")));
						suppliedProduct.setUnitsInStock(Util.getIntegerValue(r2.getAs("unitsInStock")));
						suppliedProduct.setUnitsOnOrder(Util.getIntegerValue(r2.getAs("unitsOnOrder")));
						suppliedProduct.setReorderLevel(Util.getIntegerValue(r2.getAs("reorderLevel")));
						suppliedProduct.setDiscontinued(Util.getBooleanValue(r2.getAs("discontinued")));
					} 
					if(o instanceof Products) {
						suppliedProduct = (Products) o;
					}
				}
	
				res.setSuppliedProduct(suppliedProduct);
	
				Integer productId = Util.getIntegerValue(r.getAs("A_productId"));
				if (suppliedProduct.getProductId() != null && productId != null && !suppliedProduct.getProductId().equals(productId)) {
					res.addLogEvent("Data consistency problem for [Supply - different values found for attribute 'Supply.productId': " + suppliedProduct.getProductId() + " and " + productId + "." );
					logger.warn("Data consistency problem for [Supply - different values found for attribute 'Supply.productId': " + suppliedProduct.getProductId() + " and " + productId + "." );
				}
				if(productId != null)
					suppliedProduct.setProductId(productId);
				String productName = Util.getStringValue(r.getAs("A_productName"));
				if (suppliedProduct.getProductName() != null && productName != null && !suppliedProduct.getProductName().equals(productName)) {
					res.addLogEvent("Data consistency problem for [Supply - different values found for attribute 'Supply.productName': " + suppliedProduct.getProductName() + " and " + productName + "." );
					logger.warn("Data consistency problem for [Supply - different values found for attribute 'Supply.productName': " + suppliedProduct.getProductName() + " and " + productName + "." );
				}
				if(productName != null)
					suppliedProduct.setProductName(productName);
				String quantityPerUnit = Util.getStringValue(r.getAs("A_quantityPerUnit"));
				if (suppliedProduct.getQuantityPerUnit() != null && quantityPerUnit != null && !suppliedProduct.getQuantityPerUnit().equals(quantityPerUnit)) {
					res.addLogEvent("Data consistency problem for [Supply - different values found for attribute 'Supply.quantityPerUnit': " + suppliedProduct.getQuantityPerUnit() + " and " + quantityPerUnit + "." );
					logger.warn("Data consistency problem for [Supply - different values found for attribute 'Supply.quantityPerUnit': " + suppliedProduct.getQuantityPerUnit() + " and " + quantityPerUnit + "." );
				}
				if(quantityPerUnit != null)
					suppliedProduct.setQuantityPerUnit(quantityPerUnit);
				Double unitPrice = Util.getDoubleValue(r.getAs("A_unitPrice"));
				if (suppliedProduct.getUnitPrice() != null && unitPrice != null && !suppliedProduct.getUnitPrice().equals(unitPrice)) {
					res.addLogEvent("Data consistency problem for [Supply - different values found for attribute 'Supply.unitPrice': " + suppliedProduct.getUnitPrice() + " and " + unitPrice + "." );
					logger.warn("Data consistency problem for [Supply - different values found for attribute 'Supply.unitPrice': " + suppliedProduct.getUnitPrice() + " and " + unitPrice + "." );
				}
				if(unitPrice != null)
					suppliedProduct.setUnitPrice(unitPrice);
				Integer unitsInStock = Util.getIntegerValue(r.getAs("A_unitsInStock"));
				if (suppliedProduct.getUnitsInStock() != null && unitsInStock != null && !suppliedProduct.getUnitsInStock().equals(unitsInStock)) {
					res.addLogEvent("Data consistency problem for [Supply - different values found for attribute 'Supply.unitsInStock': " + suppliedProduct.getUnitsInStock() + " and " + unitsInStock + "." );
					logger.warn("Data consistency problem for [Supply - different values found for attribute 'Supply.unitsInStock': " + suppliedProduct.getUnitsInStock() + " and " + unitsInStock + "." );
				}
				if(unitsInStock != null)
					suppliedProduct.setUnitsInStock(unitsInStock);
				Integer unitsOnOrder = Util.getIntegerValue(r.getAs("A_unitsOnOrder"));
				if (suppliedProduct.getUnitsOnOrder() != null && unitsOnOrder != null && !suppliedProduct.getUnitsOnOrder().equals(unitsOnOrder)) {
					res.addLogEvent("Data consistency problem for [Supply - different values found for attribute 'Supply.unitsOnOrder': " + suppliedProduct.getUnitsOnOrder() + " and " + unitsOnOrder + "." );
					logger.warn("Data consistency problem for [Supply - different values found for attribute 'Supply.unitsOnOrder': " + suppliedProduct.getUnitsOnOrder() + " and " + unitsOnOrder + "." );
				}
				if(unitsOnOrder != null)
					suppliedProduct.setUnitsOnOrder(unitsOnOrder);
				Integer reorderLevel = Util.getIntegerValue(r.getAs("A_reorderLevel"));
				if (suppliedProduct.getReorderLevel() != null && reorderLevel != null && !suppliedProduct.getReorderLevel().equals(reorderLevel)) {
					res.addLogEvent("Data consistency problem for [Supply - different values found for attribute 'Supply.reorderLevel': " + suppliedProduct.getReorderLevel() + " and " + reorderLevel + "." );
					logger.warn("Data consistency problem for [Supply - different values found for attribute 'Supply.reorderLevel': " + suppliedProduct.getReorderLevel() + " and " + reorderLevel + "." );
				}
				if(reorderLevel != null)
					suppliedProduct.setReorderLevel(reorderLevel);
				Boolean discontinued = Util.getBooleanValue(r.getAs("A_discontinued"));
				if (suppliedProduct.getDiscontinued() != null && discontinued != null && !suppliedProduct.getDiscontinued().equals(discontinued)) {
					res.addLogEvent("Data consistency problem for [Supply - different values found for attribute 'Supply.discontinued': " + suppliedProduct.getDiscontinued() + " and " + discontinued + "." );
					logger.warn("Data consistency problem for [Supply - different values found for attribute 'Supply.discontinued': " + suppliedProduct.getDiscontinued() + " and " + discontinued + "." );
				}
				if(discontinued != null)
					suppliedProduct.setDiscontinued(discontinued);
	
				o = r.getAs("supplier");
				Suppliers supplier = new Suppliers();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						supplier.setSupplierId(Util.getIntegerValue(r2.getAs("supplierId")));
						supplier.setCompanyName(Util.getStringValue(r2.getAs("companyName")));
						supplier.setContactName(Util.getStringValue(r2.getAs("contactName")));
						supplier.setContactTitle(Util.getStringValue(r2.getAs("contactTitle")));
						supplier.setAddress(Util.getStringValue(r2.getAs("address")));
						supplier.setCity(Util.getStringValue(r2.getAs("city")));
						supplier.setRegion(Util.getStringValue(r2.getAs("region")));
						supplier.setPostalCode(Util.getStringValue(r2.getAs("postalCode")));
						supplier.setCountry(Util.getStringValue(r2.getAs("country")));
						supplier.setPhone(Util.getStringValue(r2.getAs("phone")));
						supplier.setFax(Util.getStringValue(r2.getAs("fax")));
						supplier.setHomePage(Util.getStringValue(r2.getAs("homePage")));
					} 
					if(o instanceof Suppliers) {
						supplier = (Suppliers) o;
					}
				}
	
				res.setSupplier(supplier);
	
				return res;
		}, Encoders.bean(Supply.class));
	
		
		
	}
	public static Dataset<Supply> fullLeftOuterJoinBetweenSupplyAndSupplier(Dataset<Supply> d1, Dataset<Suppliers> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("supplierId", "A_supplierId")
			.withColumnRenamed("companyName", "A_companyName")
			.withColumnRenamed("contactName", "A_contactName")
			.withColumnRenamed("contactTitle", "A_contactTitle")
			.withColumnRenamed("address", "A_address")
			.withColumnRenamed("city", "A_city")
			.withColumnRenamed("region", "A_region")
			.withColumnRenamed("postalCode", "A_postalCode")
			.withColumnRenamed("country", "A_country")
			.withColumnRenamed("phone", "A_phone")
			.withColumnRenamed("fax", "A_fax")
			.withColumnRenamed("homePage", "A_homePage")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("supplier.supplierId").equalTo(d2_.col("A_supplierId"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Supply>) r -> {
				Supply res = new Supply();
	
				Suppliers supplier = new Suppliers();
				Object o = r.getAs("supplier");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						supplier.setSupplierId(Util.getIntegerValue(r2.getAs("supplierId")));
						supplier.setCompanyName(Util.getStringValue(r2.getAs("companyName")));
						supplier.setContactName(Util.getStringValue(r2.getAs("contactName")));
						supplier.setContactTitle(Util.getStringValue(r2.getAs("contactTitle")));
						supplier.setAddress(Util.getStringValue(r2.getAs("address")));
						supplier.setCity(Util.getStringValue(r2.getAs("city")));
						supplier.setRegion(Util.getStringValue(r2.getAs("region")));
						supplier.setPostalCode(Util.getStringValue(r2.getAs("postalCode")));
						supplier.setCountry(Util.getStringValue(r2.getAs("country")));
						supplier.setPhone(Util.getStringValue(r2.getAs("phone")));
						supplier.setFax(Util.getStringValue(r2.getAs("fax")));
						supplier.setHomePage(Util.getStringValue(r2.getAs("homePage")));
					} 
					if(o instanceof Suppliers) {
						supplier = (Suppliers) o;
					}
				}
	
				res.setSupplier(supplier);
	
				Integer supplierId = Util.getIntegerValue(r.getAs("A_supplierId"));
				if (supplier.getSupplierId() != null && supplierId != null && !supplier.getSupplierId().equals(supplierId)) {
					res.addLogEvent("Data consistency problem for [Supply - different values found for attribute 'Supply.supplierId': " + supplier.getSupplierId() + " and " + supplierId + "." );
					logger.warn("Data consistency problem for [Supply - different values found for attribute 'Supply.supplierId': " + supplier.getSupplierId() + " and " + supplierId + "." );
				}
				if(supplierId != null)
					supplier.setSupplierId(supplierId);
				String companyName = Util.getStringValue(r.getAs("A_companyName"));
				if (supplier.getCompanyName() != null && companyName != null && !supplier.getCompanyName().equals(companyName)) {
					res.addLogEvent("Data consistency problem for [Supply - different values found for attribute 'Supply.companyName': " + supplier.getCompanyName() + " and " + companyName + "." );
					logger.warn("Data consistency problem for [Supply - different values found for attribute 'Supply.companyName': " + supplier.getCompanyName() + " and " + companyName + "." );
				}
				if(companyName != null)
					supplier.setCompanyName(companyName);
				String contactName = Util.getStringValue(r.getAs("A_contactName"));
				if (supplier.getContactName() != null && contactName != null && !supplier.getContactName().equals(contactName)) {
					res.addLogEvent("Data consistency problem for [Supply - different values found for attribute 'Supply.contactName': " + supplier.getContactName() + " and " + contactName + "." );
					logger.warn("Data consistency problem for [Supply - different values found for attribute 'Supply.contactName': " + supplier.getContactName() + " and " + contactName + "." );
				}
				if(contactName != null)
					supplier.setContactName(contactName);
				String contactTitle = Util.getStringValue(r.getAs("A_contactTitle"));
				if (supplier.getContactTitle() != null && contactTitle != null && !supplier.getContactTitle().equals(contactTitle)) {
					res.addLogEvent("Data consistency problem for [Supply - different values found for attribute 'Supply.contactTitle': " + supplier.getContactTitle() + " and " + contactTitle + "." );
					logger.warn("Data consistency problem for [Supply - different values found for attribute 'Supply.contactTitle': " + supplier.getContactTitle() + " and " + contactTitle + "." );
				}
				if(contactTitle != null)
					supplier.setContactTitle(contactTitle);
				String address = Util.getStringValue(r.getAs("A_address"));
				if (supplier.getAddress() != null && address != null && !supplier.getAddress().equals(address)) {
					res.addLogEvent("Data consistency problem for [Supply - different values found for attribute 'Supply.address': " + supplier.getAddress() + " and " + address + "." );
					logger.warn("Data consistency problem for [Supply - different values found for attribute 'Supply.address': " + supplier.getAddress() + " and " + address + "." );
				}
				if(address != null)
					supplier.setAddress(address);
				String city = Util.getStringValue(r.getAs("A_city"));
				if (supplier.getCity() != null && city != null && !supplier.getCity().equals(city)) {
					res.addLogEvent("Data consistency problem for [Supply - different values found for attribute 'Supply.city': " + supplier.getCity() + " and " + city + "." );
					logger.warn("Data consistency problem for [Supply - different values found for attribute 'Supply.city': " + supplier.getCity() + " and " + city + "." );
				}
				if(city != null)
					supplier.setCity(city);
				String region = Util.getStringValue(r.getAs("A_region"));
				if (supplier.getRegion() != null && region != null && !supplier.getRegion().equals(region)) {
					res.addLogEvent("Data consistency problem for [Supply - different values found for attribute 'Supply.region': " + supplier.getRegion() + " and " + region + "." );
					logger.warn("Data consistency problem for [Supply - different values found for attribute 'Supply.region': " + supplier.getRegion() + " and " + region + "." );
				}
				if(region != null)
					supplier.setRegion(region);
				String postalCode = Util.getStringValue(r.getAs("A_postalCode"));
				if (supplier.getPostalCode() != null && postalCode != null && !supplier.getPostalCode().equals(postalCode)) {
					res.addLogEvent("Data consistency problem for [Supply - different values found for attribute 'Supply.postalCode': " + supplier.getPostalCode() + " and " + postalCode + "." );
					logger.warn("Data consistency problem for [Supply - different values found for attribute 'Supply.postalCode': " + supplier.getPostalCode() + " and " + postalCode + "." );
				}
				if(postalCode != null)
					supplier.setPostalCode(postalCode);
				String country = Util.getStringValue(r.getAs("A_country"));
				if (supplier.getCountry() != null && country != null && !supplier.getCountry().equals(country)) {
					res.addLogEvent("Data consistency problem for [Supply - different values found for attribute 'Supply.country': " + supplier.getCountry() + " and " + country + "." );
					logger.warn("Data consistency problem for [Supply - different values found for attribute 'Supply.country': " + supplier.getCountry() + " and " + country + "." );
				}
				if(country != null)
					supplier.setCountry(country);
				String phone = Util.getStringValue(r.getAs("A_phone"));
				if (supplier.getPhone() != null && phone != null && !supplier.getPhone().equals(phone)) {
					res.addLogEvent("Data consistency problem for [Supply - different values found for attribute 'Supply.phone': " + supplier.getPhone() + " and " + phone + "." );
					logger.warn("Data consistency problem for [Supply - different values found for attribute 'Supply.phone': " + supplier.getPhone() + " and " + phone + "." );
				}
				if(phone != null)
					supplier.setPhone(phone);
				String fax = Util.getStringValue(r.getAs("A_fax"));
				if (supplier.getFax() != null && fax != null && !supplier.getFax().equals(fax)) {
					res.addLogEvent("Data consistency problem for [Supply - different values found for attribute 'Supply.fax': " + supplier.getFax() + " and " + fax + "." );
					logger.warn("Data consistency problem for [Supply - different values found for attribute 'Supply.fax': " + supplier.getFax() + " and " + fax + "." );
				}
				if(fax != null)
					supplier.setFax(fax);
				String homePage = Util.getStringValue(r.getAs("A_homePage"));
				if (supplier.getHomePage() != null && homePage != null && !supplier.getHomePage().equals(homePage)) {
					res.addLogEvent("Data consistency problem for [Supply - different values found for attribute 'Supply.homePage': " + supplier.getHomePage() + " and " + homePage + "." );
					logger.warn("Data consistency problem for [Supply - different values found for attribute 'Supply.homePage': " + supplier.getHomePage() + " and " + homePage + "." );
				}
				if(homePage != null)
					supplier.setHomePage(homePage);
	
				o = r.getAs("suppliedProduct");
				Products suppliedProduct = new Products();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						suppliedProduct.setProductId(Util.getIntegerValue(r2.getAs("productId")));
						suppliedProduct.setProductName(Util.getStringValue(r2.getAs("productName")));
						suppliedProduct.setQuantityPerUnit(Util.getStringValue(r2.getAs("quantityPerUnit")));
						suppliedProduct.setUnitPrice(Util.getDoubleValue(r2.getAs("unitPrice")));
						suppliedProduct.setUnitsInStock(Util.getIntegerValue(r2.getAs("unitsInStock")));
						suppliedProduct.setUnitsOnOrder(Util.getIntegerValue(r2.getAs("unitsOnOrder")));
						suppliedProduct.setReorderLevel(Util.getIntegerValue(r2.getAs("reorderLevel")));
						suppliedProduct.setDiscontinued(Util.getBooleanValue(r2.getAs("discontinued")));
					} 
					if(o instanceof Products) {
						suppliedProduct = (Products) o;
					}
				}
	
				res.setSuppliedProduct(suppliedProduct);
	
				return res;
		}, Encoders.bean(Supply.class));
	
		
		
	}
	
	public static Dataset<Supply> fullOuterJoinsSupply(List<Dataset<Supply>> datasetsPOJO) {
		return fullOuterJoinsSupply(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Supply> fullLeftOuterJoinsSupply(List<Dataset<Supply>> datasetsPOJO) {
		return fullOuterJoinsSupply(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Supply> fullOuterJoinsSupply(List<Dataset<Supply>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		List<String> idFields = new ArrayList<String>();
		idFields.add("suppliedProduct.productId");
	
		idFields.add("supplier.supplierId");
		scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
		
		List<Dataset<Row>> rows = new ArrayList<Dataset<Row>>();
		for(int i = 0; i < datasetsPOJO.size(); i++) {
			Dataset<Supply> d = datasetsPOJO.get(i);
			rows.add(d
				.withColumn("suppliedProduct_productId_" + i, d.col("suppliedProduct.productId"))
				.withColumn("supplier_supplierId_" + i, d.col("supplier.supplierId"))
				.withColumnRenamed("suppliedProduct", "suppliedProduct_" + i)
				.withColumnRenamed("supplier", "supplier_" + i)
				.withColumnRenamed("logEvents", "logEvents_" + i));
		}
		
		Column joinCond;
		joinCond = rows.get(0).col("suppliedProduct_productId_0").equalTo(rows.get(1).col("suppliedProduct_productId_1"));
		joinCond = joinCond.and(rows.get(0).col("supplier_supplierId_0").equalTo(rows.get(1).col("supplier_supplierId_1")));
		
		Dataset<Row> res = rows.get(0).join(rows.get(1), joinCond, joinMode);
		for(int i = 2; i < rows.size(); i++) {
			joinCond = rows.get(i - 1).col("suppliedProduct_productId_" + (i - 1)).equalTo(rows.get(i).col("suppliedProduct_productId_" + i));
			joinCond = joinCond.and(rows.get(i - 1).col("supplier_supplierId_" + (i - 1)).equalTo(rows.get(i).col("supplier_supplierId_" + i)));
			res = res.join(rows.get(i), joinCond, joinMode);
		}
	
		return res.map((MapFunction<Row, Supply>) r -> {
				Supply supply_res = new Supply();
	
					WrappedArray logEvents = r.getAs("logEvents_0");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							supply_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							supply_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					Products suppliedProduct_res = new Products();
					Suppliers supplier_res = new Suppliers();
					
					// attribute 'Products.productId'
					Integer firstNotNull_suppliedProduct_productId = Util.getIntegerValue(r.getAs("suppliedProduct_0.productId"));
					suppliedProduct_res.setProductId(firstNotNull_suppliedProduct_productId);
					// attribute 'Products.productName'
					String firstNotNull_suppliedProduct_ProductName = Util.getStringValue(r.getAs("suppliedProduct_0.productName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String suppliedProduct_productName2 = Util.getStringValue(r.getAs("suppliedProduct_" + i + ".productName"));
						if (firstNotNull_suppliedProduct_ProductName != null && suppliedProduct_productName2 != null && !firstNotNull_suppliedProduct_ProductName.equals(suppliedProduct_productName2)) {
							supply_res.addLogEvent("Data consistency problem for [Products - id :"+suppliedProduct_res.getProductId()+"]: different values found for attribute 'Products.productName': " + firstNotNull_suppliedProduct_ProductName + " and " + suppliedProduct_productName2 + "." );
							logger.warn("Data consistency problem for [Products - id :"+suppliedProduct_res.getProductId()+"]: different values found for attribute 'Products.productName': " + firstNotNull_suppliedProduct_ProductName + " and " + suppliedProduct_productName2 + "." );
						}
						if (firstNotNull_suppliedProduct_ProductName == null && suppliedProduct_productName2 != null) {
							firstNotNull_suppliedProduct_ProductName = suppliedProduct_productName2;
						}
					}
					suppliedProduct_res.setProductName(firstNotNull_suppliedProduct_ProductName);
					// attribute 'Products.quantityPerUnit'
					String firstNotNull_suppliedProduct_QuantityPerUnit = Util.getStringValue(r.getAs("suppliedProduct_0.quantityPerUnit"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String suppliedProduct_quantityPerUnit2 = Util.getStringValue(r.getAs("suppliedProduct_" + i + ".quantityPerUnit"));
						if (firstNotNull_suppliedProduct_QuantityPerUnit != null && suppliedProduct_quantityPerUnit2 != null && !firstNotNull_suppliedProduct_QuantityPerUnit.equals(suppliedProduct_quantityPerUnit2)) {
							supply_res.addLogEvent("Data consistency problem for [Products - id :"+suppliedProduct_res.getProductId()+"]: different values found for attribute 'Products.quantityPerUnit': " + firstNotNull_suppliedProduct_QuantityPerUnit + " and " + suppliedProduct_quantityPerUnit2 + "." );
							logger.warn("Data consistency problem for [Products - id :"+suppliedProduct_res.getProductId()+"]: different values found for attribute 'Products.quantityPerUnit': " + firstNotNull_suppliedProduct_QuantityPerUnit + " and " + suppliedProduct_quantityPerUnit2 + "." );
						}
						if (firstNotNull_suppliedProduct_QuantityPerUnit == null && suppliedProduct_quantityPerUnit2 != null) {
							firstNotNull_suppliedProduct_QuantityPerUnit = suppliedProduct_quantityPerUnit2;
						}
					}
					suppliedProduct_res.setQuantityPerUnit(firstNotNull_suppliedProduct_QuantityPerUnit);
					// attribute 'Products.unitPrice'
					Double firstNotNull_suppliedProduct_UnitPrice = Util.getDoubleValue(r.getAs("suppliedProduct_0.unitPrice"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double suppliedProduct_unitPrice2 = Util.getDoubleValue(r.getAs("suppliedProduct_" + i + ".unitPrice"));
						if (firstNotNull_suppliedProduct_UnitPrice != null && suppliedProduct_unitPrice2 != null && !firstNotNull_suppliedProduct_UnitPrice.equals(suppliedProduct_unitPrice2)) {
							supply_res.addLogEvent("Data consistency problem for [Products - id :"+suppliedProduct_res.getProductId()+"]: different values found for attribute 'Products.unitPrice': " + firstNotNull_suppliedProduct_UnitPrice + " and " + suppliedProduct_unitPrice2 + "." );
							logger.warn("Data consistency problem for [Products - id :"+suppliedProduct_res.getProductId()+"]: different values found for attribute 'Products.unitPrice': " + firstNotNull_suppliedProduct_UnitPrice + " and " + suppliedProduct_unitPrice2 + "." );
						}
						if (firstNotNull_suppliedProduct_UnitPrice == null && suppliedProduct_unitPrice2 != null) {
							firstNotNull_suppliedProduct_UnitPrice = suppliedProduct_unitPrice2;
						}
					}
					suppliedProduct_res.setUnitPrice(firstNotNull_suppliedProduct_UnitPrice);
					// attribute 'Products.unitsInStock'
					Integer firstNotNull_suppliedProduct_UnitsInStock = Util.getIntegerValue(r.getAs("suppliedProduct_0.unitsInStock"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer suppliedProduct_unitsInStock2 = Util.getIntegerValue(r.getAs("suppliedProduct_" + i + ".unitsInStock"));
						if (firstNotNull_suppliedProduct_UnitsInStock != null && suppliedProduct_unitsInStock2 != null && !firstNotNull_suppliedProduct_UnitsInStock.equals(suppliedProduct_unitsInStock2)) {
							supply_res.addLogEvent("Data consistency problem for [Products - id :"+suppliedProduct_res.getProductId()+"]: different values found for attribute 'Products.unitsInStock': " + firstNotNull_suppliedProduct_UnitsInStock + " and " + suppliedProduct_unitsInStock2 + "." );
							logger.warn("Data consistency problem for [Products - id :"+suppliedProduct_res.getProductId()+"]: different values found for attribute 'Products.unitsInStock': " + firstNotNull_suppliedProduct_UnitsInStock + " and " + suppliedProduct_unitsInStock2 + "." );
						}
						if (firstNotNull_suppliedProduct_UnitsInStock == null && suppliedProduct_unitsInStock2 != null) {
							firstNotNull_suppliedProduct_UnitsInStock = suppliedProduct_unitsInStock2;
						}
					}
					suppliedProduct_res.setUnitsInStock(firstNotNull_suppliedProduct_UnitsInStock);
					// attribute 'Products.unitsOnOrder'
					Integer firstNotNull_suppliedProduct_UnitsOnOrder = Util.getIntegerValue(r.getAs("suppliedProduct_0.unitsOnOrder"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer suppliedProduct_unitsOnOrder2 = Util.getIntegerValue(r.getAs("suppliedProduct_" + i + ".unitsOnOrder"));
						if (firstNotNull_suppliedProduct_UnitsOnOrder != null && suppliedProduct_unitsOnOrder2 != null && !firstNotNull_suppliedProduct_UnitsOnOrder.equals(suppliedProduct_unitsOnOrder2)) {
							supply_res.addLogEvent("Data consistency problem for [Products - id :"+suppliedProduct_res.getProductId()+"]: different values found for attribute 'Products.unitsOnOrder': " + firstNotNull_suppliedProduct_UnitsOnOrder + " and " + suppliedProduct_unitsOnOrder2 + "." );
							logger.warn("Data consistency problem for [Products - id :"+suppliedProduct_res.getProductId()+"]: different values found for attribute 'Products.unitsOnOrder': " + firstNotNull_suppliedProduct_UnitsOnOrder + " and " + suppliedProduct_unitsOnOrder2 + "." );
						}
						if (firstNotNull_suppliedProduct_UnitsOnOrder == null && suppliedProduct_unitsOnOrder2 != null) {
							firstNotNull_suppliedProduct_UnitsOnOrder = suppliedProduct_unitsOnOrder2;
						}
					}
					suppliedProduct_res.setUnitsOnOrder(firstNotNull_suppliedProduct_UnitsOnOrder);
					// attribute 'Products.reorderLevel'
					Integer firstNotNull_suppliedProduct_ReorderLevel = Util.getIntegerValue(r.getAs("suppliedProduct_0.reorderLevel"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer suppliedProduct_reorderLevel2 = Util.getIntegerValue(r.getAs("suppliedProduct_" + i + ".reorderLevel"));
						if (firstNotNull_suppliedProduct_ReorderLevel != null && suppliedProduct_reorderLevel2 != null && !firstNotNull_suppliedProduct_ReorderLevel.equals(suppliedProduct_reorderLevel2)) {
							supply_res.addLogEvent("Data consistency problem for [Products - id :"+suppliedProduct_res.getProductId()+"]: different values found for attribute 'Products.reorderLevel': " + firstNotNull_suppliedProduct_ReorderLevel + " and " + suppliedProduct_reorderLevel2 + "." );
							logger.warn("Data consistency problem for [Products - id :"+suppliedProduct_res.getProductId()+"]: different values found for attribute 'Products.reorderLevel': " + firstNotNull_suppliedProduct_ReorderLevel + " and " + suppliedProduct_reorderLevel2 + "." );
						}
						if (firstNotNull_suppliedProduct_ReorderLevel == null && suppliedProduct_reorderLevel2 != null) {
							firstNotNull_suppliedProduct_ReorderLevel = suppliedProduct_reorderLevel2;
						}
					}
					suppliedProduct_res.setReorderLevel(firstNotNull_suppliedProduct_ReorderLevel);
					// attribute 'Products.discontinued'
					Boolean firstNotNull_suppliedProduct_Discontinued = Util.getBooleanValue(r.getAs("suppliedProduct_0.discontinued"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Boolean suppliedProduct_discontinued2 = Util.getBooleanValue(r.getAs("suppliedProduct_" + i + ".discontinued"));
						if (firstNotNull_suppliedProduct_Discontinued != null && suppliedProduct_discontinued2 != null && !firstNotNull_suppliedProduct_Discontinued.equals(suppliedProduct_discontinued2)) {
							supply_res.addLogEvent("Data consistency problem for [Products - id :"+suppliedProduct_res.getProductId()+"]: different values found for attribute 'Products.discontinued': " + firstNotNull_suppliedProduct_Discontinued + " and " + suppliedProduct_discontinued2 + "." );
							logger.warn("Data consistency problem for [Products - id :"+suppliedProduct_res.getProductId()+"]: different values found for attribute 'Products.discontinued': " + firstNotNull_suppliedProduct_Discontinued + " and " + suppliedProduct_discontinued2 + "." );
						}
						if (firstNotNull_suppliedProduct_Discontinued == null && suppliedProduct_discontinued2 != null) {
							firstNotNull_suppliedProduct_Discontinued = suppliedProduct_discontinued2;
						}
					}
					suppliedProduct_res.setDiscontinued(firstNotNull_suppliedProduct_Discontinued);
					// attribute 'Suppliers.supplierId'
					Integer firstNotNull_supplier_supplierId = Util.getIntegerValue(r.getAs("supplier_0.supplierId"));
					supplier_res.setSupplierId(firstNotNull_supplier_supplierId);
					// attribute 'Suppliers.companyName'
					String firstNotNull_supplier_CompanyName = Util.getStringValue(r.getAs("supplier_0.companyName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplier_companyName2 = Util.getStringValue(r.getAs("supplier_" + i + ".companyName"));
						if (firstNotNull_supplier_CompanyName != null && supplier_companyName2 != null && !firstNotNull_supplier_CompanyName.equals(supplier_companyName2)) {
							supply_res.addLogEvent("Data consistency problem for [Suppliers - id :"+supplier_res.getSupplierId()+"]: different values found for attribute 'Suppliers.companyName': " + firstNotNull_supplier_CompanyName + " and " + supplier_companyName2 + "." );
							logger.warn("Data consistency problem for [Suppliers - id :"+supplier_res.getSupplierId()+"]: different values found for attribute 'Suppliers.companyName': " + firstNotNull_supplier_CompanyName + " and " + supplier_companyName2 + "." );
						}
						if (firstNotNull_supplier_CompanyName == null && supplier_companyName2 != null) {
							firstNotNull_supplier_CompanyName = supplier_companyName2;
						}
					}
					supplier_res.setCompanyName(firstNotNull_supplier_CompanyName);
					// attribute 'Suppliers.contactName'
					String firstNotNull_supplier_ContactName = Util.getStringValue(r.getAs("supplier_0.contactName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplier_contactName2 = Util.getStringValue(r.getAs("supplier_" + i + ".contactName"));
						if (firstNotNull_supplier_ContactName != null && supplier_contactName2 != null && !firstNotNull_supplier_ContactName.equals(supplier_contactName2)) {
							supply_res.addLogEvent("Data consistency problem for [Suppliers - id :"+supplier_res.getSupplierId()+"]: different values found for attribute 'Suppliers.contactName': " + firstNotNull_supplier_ContactName + " and " + supplier_contactName2 + "." );
							logger.warn("Data consistency problem for [Suppliers - id :"+supplier_res.getSupplierId()+"]: different values found for attribute 'Suppliers.contactName': " + firstNotNull_supplier_ContactName + " and " + supplier_contactName2 + "." );
						}
						if (firstNotNull_supplier_ContactName == null && supplier_contactName2 != null) {
							firstNotNull_supplier_ContactName = supplier_contactName2;
						}
					}
					supplier_res.setContactName(firstNotNull_supplier_ContactName);
					// attribute 'Suppliers.contactTitle'
					String firstNotNull_supplier_ContactTitle = Util.getStringValue(r.getAs("supplier_0.contactTitle"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplier_contactTitle2 = Util.getStringValue(r.getAs("supplier_" + i + ".contactTitle"));
						if (firstNotNull_supplier_ContactTitle != null && supplier_contactTitle2 != null && !firstNotNull_supplier_ContactTitle.equals(supplier_contactTitle2)) {
							supply_res.addLogEvent("Data consistency problem for [Suppliers - id :"+supplier_res.getSupplierId()+"]: different values found for attribute 'Suppliers.contactTitle': " + firstNotNull_supplier_ContactTitle + " and " + supplier_contactTitle2 + "." );
							logger.warn("Data consistency problem for [Suppliers - id :"+supplier_res.getSupplierId()+"]: different values found for attribute 'Suppliers.contactTitle': " + firstNotNull_supplier_ContactTitle + " and " + supplier_contactTitle2 + "." );
						}
						if (firstNotNull_supplier_ContactTitle == null && supplier_contactTitle2 != null) {
							firstNotNull_supplier_ContactTitle = supplier_contactTitle2;
						}
					}
					supplier_res.setContactTitle(firstNotNull_supplier_ContactTitle);
					// attribute 'Suppliers.address'
					String firstNotNull_supplier_Address = Util.getStringValue(r.getAs("supplier_0.address"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplier_address2 = Util.getStringValue(r.getAs("supplier_" + i + ".address"));
						if (firstNotNull_supplier_Address != null && supplier_address2 != null && !firstNotNull_supplier_Address.equals(supplier_address2)) {
							supply_res.addLogEvent("Data consistency problem for [Suppliers - id :"+supplier_res.getSupplierId()+"]: different values found for attribute 'Suppliers.address': " + firstNotNull_supplier_Address + " and " + supplier_address2 + "." );
							logger.warn("Data consistency problem for [Suppliers - id :"+supplier_res.getSupplierId()+"]: different values found for attribute 'Suppliers.address': " + firstNotNull_supplier_Address + " and " + supplier_address2 + "." );
						}
						if (firstNotNull_supplier_Address == null && supplier_address2 != null) {
							firstNotNull_supplier_Address = supplier_address2;
						}
					}
					supplier_res.setAddress(firstNotNull_supplier_Address);
					// attribute 'Suppliers.city'
					String firstNotNull_supplier_City = Util.getStringValue(r.getAs("supplier_0.city"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplier_city2 = Util.getStringValue(r.getAs("supplier_" + i + ".city"));
						if (firstNotNull_supplier_City != null && supplier_city2 != null && !firstNotNull_supplier_City.equals(supplier_city2)) {
							supply_res.addLogEvent("Data consistency problem for [Suppliers - id :"+supplier_res.getSupplierId()+"]: different values found for attribute 'Suppliers.city': " + firstNotNull_supplier_City + " and " + supplier_city2 + "." );
							logger.warn("Data consistency problem for [Suppliers - id :"+supplier_res.getSupplierId()+"]: different values found for attribute 'Suppliers.city': " + firstNotNull_supplier_City + " and " + supplier_city2 + "." );
						}
						if (firstNotNull_supplier_City == null && supplier_city2 != null) {
							firstNotNull_supplier_City = supplier_city2;
						}
					}
					supplier_res.setCity(firstNotNull_supplier_City);
					// attribute 'Suppliers.region'
					String firstNotNull_supplier_Region = Util.getStringValue(r.getAs("supplier_0.region"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplier_region2 = Util.getStringValue(r.getAs("supplier_" + i + ".region"));
						if (firstNotNull_supplier_Region != null && supplier_region2 != null && !firstNotNull_supplier_Region.equals(supplier_region2)) {
							supply_res.addLogEvent("Data consistency problem for [Suppliers - id :"+supplier_res.getSupplierId()+"]: different values found for attribute 'Suppliers.region': " + firstNotNull_supplier_Region + " and " + supplier_region2 + "." );
							logger.warn("Data consistency problem for [Suppliers - id :"+supplier_res.getSupplierId()+"]: different values found for attribute 'Suppliers.region': " + firstNotNull_supplier_Region + " and " + supplier_region2 + "." );
						}
						if (firstNotNull_supplier_Region == null && supplier_region2 != null) {
							firstNotNull_supplier_Region = supplier_region2;
						}
					}
					supplier_res.setRegion(firstNotNull_supplier_Region);
					// attribute 'Suppliers.postalCode'
					String firstNotNull_supplier_PostalCode = Util.getStringValue(r.getAs("supplier_0.postalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplier_postalCode2 = Util.getStringValue(r.getAs("supplier_" + i + ".postalCode"));
						if (firstNotNull_supplier_PostalCode != null && supplier_postalCode2 != null && !firstNotNull_supplier_PostalCode.equals(supplier_postalCode2)) {
							supply_res.addLogEvent("Data consistency problem for [Suppliers - id :"+supplier_res.getSupplierId()+"]: different values found for attribute 'Suppliers.postalCode': " + firstNotNull_supplier_PostalCode + " and " + supplier_postalCode2 + "." );
							logger.warn("Data consistency problem for [Suppliers - id :"+supplier_res.getSupplierId()+"]: different values found for attribute 'Suppliers.postalCode': " + firstNotNull_supplier_PostalCode + " and " + supplier_postalCode2 + "." );
						}
						if (firstNotNull_supplier_PostalCode == null && supplier_postalCode2 != null) {
							firstNotNull_supplier_PostalCode = supplier_postalCode2;
						}
					}
					supplier_res.setPostalCode(firstNotNull_supplier_PostalCode);
					// attribute 'Suppliers.country'
					String firstNotNull_supplier_Country = Util.getStringValue(r.getAs("supplier_0.country"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplier_country2 = Util.getStringValue(r.getAs("supplier_" + i + ".country"));
						if (firstNotNull_supplier_Country != null && supplier_country2 != null && !firstNotNull_supplier_Country.equals(supplier_country2)) {
							supply_res.addLogEvent("Data consistency problem for [Suppliers - id :"+supplier_res.getSupplierId()+"]: different values found for attribute 'Suppliers.country': " + firstNotNull_supplier_Country + " and " + supplier_country2 + "." );
							logger.warn("Data consistency problem for [Suppliers - id :"+supplier_res.getSupplierId()+"]: different values found for attribute 'Suppliers.country': " + firstNotNull_supplier_Country + " and " + supplier_country2 + "." );
						}
						if (firstNotNull_supplier_Country == null && supplier_country2 != null) {
							firstNotNull_supplier_Country = supplier_country2;
						}
					}
					supplier_res.setCountry(firstNotNull_supplier_Country);
					// attribute 'Suppliers.phone'
					String firstNotNull_supplier_Phone = Util.getStringValue(r.getAs("supplier_0.phone"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplier_phone2 = Util.getStringValue(r.getAs("supplier_" + i + ".phone"));
						if (firstNotNull_supplier_Phone != null && supplier_phone2 != null && !firstNotNull_supplier_Phone.equals(supplier_phone2)) {
							supply_res.addLogEvent("Data consistency problem for [Suppliers - id :"+supplier_res.getSupplierId()+"]: different values found for attribute 'Suppliers.phone': " + firstNotNull_supplier_Phone + " and " + supplier_phone2 + "." );
							logger.warn("Data consistency problem for [Suppliers - id :"+supplier_res.getSupplierId()+"]: different values found for attribute 'Suppliers.phone': " + firstNotNull_supplier_Phone + " and " + supplier_phone2 + "." );
						}
						if (firstNotNull_supplier_Phone == null && supplier_phone2 != null) {
							firstNotNull_supplier_Phone = supplier_phone2;
						}
					}
					supplier_res.setPhone(firstNotNull_supplier_Phone);
					// attribute 'Suppliers.fax'
					String firstNotNull_supplier_Fax = Util.getStringValue(r.getAs("supplier_0.fax"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplier_fax2 = Util.getStringValue(r.getAs("supplier_" + i + ".fax"));
						if (firstNotNull_supplier_Fax != null && supplier_fax2 != null && !firstNotNull_supplier_Fax.equals(supplier_fax2)) {
							supply_res.addLogEvent("Data consistency problem for [Suppliers - id :"+supplier_res.getSupplierId()+"]: different values found for attribute 'Suppliers.fax': " + firstNotNull_supplier_Fax + " and " + supplier_fax2 + "." );
							logger.warn("Data consistency problem for [Suppliers - id :"+supplier_res.getSupplierId()+"]: different values found for attribute 'Suppliers.fax': " + firstNotNull_supplier_Fax + " and " + supplier_fax2 + "." );
						}
						if (firstNotNull_supplier_Fax == null && supplier_fax2 != null) {
							firstNotNull_supplier_Fax = supplier_fax2;
						}
					}
					supplier_res.setFax(firstNotNull_supplier_Fax);
					// attribute 'Suppliers.homePage'
					String firstNotNull_supplier_HomePage = Util.getStringValue(r.getAs("supplier_0.homePage"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplier_homePage2 = Util.getStringValue(r.getAs("supplier_" + i + ".homePage"));
						if (firstNotNull_supplier_HomePage != null && supplier_homePage2 != null && !firstNotNull_supplier_HomePage.equals(supplier_homePage2)) {
							supply_res.addLogEvent("Data consistency problem for [Suppliers - id :"+supplier_res.getSupplierId()+"]: different values found for attribute 'Suppliers.homePage': " + firstNotNull_supplier_HomePage + " and " + supplier_homePage2 + "." );
							logger.warn("Data consistency problem for [Suppliers - id :"+supplier_res.getSupplierId()+"]: different values found for attribute 'Suppliers.homePage': " + firstNotNull_supplier_HomePage + " and " + supplier_homePage2 + "." );
						}
						if (firstNotNull_supplier_HomePage == null && supplier_homePage2 != null) {
							firstNotNull_supplier_HomePage = supplier_homePage2;
						}
					}
					supplier_res.setHomePage(firstNotNull_supplier_HomePage);
	
					supply_res.setSuppliedProduct(suppliedProduct_res);
					supply_res.setSupplier(supplier_res);
					return supply_res;
		}
		, Encoders.bean(Supply.class));
	
	}
	
	//Empty arguments
	public Dataset<Supply> getSupplyList(){
		 return getSupplyList(null,null);
	}
	
	public abstract Dataset<Supply> getSupplyList(
		Condition<ProductsAttribute> suppliedProduct_condition,
		Condition<SuppliersAttribute> supplier_condition);
	
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
	
	public abstract void insertSupply(Supply supply);
	
	
	
	public 	abstract boolean insertSupplyInRefStructProductsInMyRelDB(Supply supply);
	
	 public void insertSupply(Products suppliedProduct ,Suppliers supplier ){
		Supply supply = new Supply();
		supply.setSuppliedProduct(suppliedProduct);
		supply.setSupplier(supplier);
		insertSupply(supply);
	}
	
	 public void insertSupply(Products products, List<Suppliers> supplierList){
		Supply supply = new Supply();
		supply.setSuppliedProduct(products);
		for(Suppliers supplier : supplierList){
			supply.setSupplier(supplier);
			insertSupply(supply);
		}
	}
	
	
	public abstract void deleteSupplyList(
		conditions.Condition<conditions.ProductsAttribute> suppliedProduct_condition,
		conditions.Condition<conditions.SuppliersAttribute> supplier_condition);
	
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
