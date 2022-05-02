package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import conditions.*;
import pojo.TypeOf;
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


public abstract class TypeOfService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TypeOfService.class);
	
	public abstract Dataset<TypeOf> getProductAndCategoryListInIsCategoryInProductsFromMyRelDB(Condition<ProductsAttribute> product_cond, Condition<CategoriesAttribute> category_cond, MutableBoolean product_refilter, MutableBoolean category_refilter);
	
	
	
	
	
	public static Dataset<TypeOf> fullLeftOuterJoinBetweenTypeOfAndProduct(Dataset<TypeOf> d1, Dataset<Products> d2) {
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
		joinCond = d1.col("product.productId").equalTo(d2_.col("A_productId"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, TypeOf>) r -> {
				TypeOf res = new TypeOf();
	
				Products product = new Products();
				Object o = r.getAs("product");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						product.setProductId(Util.getIntegerValue(r2.getAs("productId")));
						product.setProductName(Util.getStringValue(r2.getAs("productName")));
						product.setQuantityPerUnit(Util.getStringValue(r2.getAs("quantityPerUnit")));
						product.setUnitPrice(Util.getDoubleValue(r2.getAs("unitPrice")));
						product.setUnitsInStock(Util.getIntegerValue(r2.getAs("unitsInStock")));
						product.setUnitsOnOrder(Util.getIntegerValue(r2.getAs("unitsOnOrder")));
						product.setReorderLevel(Util.getIntegerValue(r2.getAs("reorderLevel")));
						product.setDiscontinued(Util.getBooleanValue(r2.getAs("discontinued")));
					} 
					if(o instanceof Products) {
						product = (Products) o;
					}
				}
	
				res.setProduct(product);
	
				Integer productId = Util.getIntegerValue(r.getAs("A_productId"));
				if (product.getProductId() != null && productId != null && !product.getProductId().equals(productId)) {
					res.addLogEvent("Data consistency problem for [TypeOf - different values found for attribute 'TypeOf.productId': " + product.getProductId() + " and " + productId + "." );
					logger.warn("Data consistency problem for [TypeOf - different values found for attribute 'TypeOf.productId': " + product.getProductId() + " and " + productId + "." );
				}
				if(productId != null)
					product.setProductId(productId);
				String productName = Util.getStringValue(r.getAs("A_productName"));
				if (product.getProductName() != null && productName != null && !product.getProductName().equals(productName)) {
					res.addLogEvent("Data consistency problem for [TypeOf - different values found for attribute 'TypeOf.productName': " + product.getProductName() + " and " + productName + "." );
					logger.warn("Data consistency problem for [TypeOf - different values found for attribute 'TypeOf.productName': " + product.getProductName() + " and " + productName + "." );
				}
				if(productName != null)
					product.setProductName(productName);
				String quantityPerUnit = Util.getStringValue(r.getAs("A_quantityPerUnit"));
				if (product.getQuantityPerUnit() != null && quantityPerUnit != null && !product.getQuantityPerUnit().equals(quantityPerUnit)) {
					res.addLogEvent("Data consistency problem for [TypeOf - different values found for attribute 'TypeOf.quantityPerUnit': " + product.getQuantityPerUnit() + " and " + quantityPerUnit + "." );
					logger.warn("Data consistency problem for [TypeOf - different values found for attribute 'TypeOf.quantityPerUnit': " + product.getQuantityPerUnit() + " and " + quantityPerUnit + "." );
				}
				if(quantityPerUnit != null)
					product.setQuantityPerUnit(quantityPerUnit);
				Double unitPrice = Util.getDoubleValue(r.getAs("A_unitPrice"));
				if (product.getUnitPrice() != null && unitPrice != null && !product.getUnitPrice().equals(unitPrice)) {
					res.addLogEvent("Data consistency problem for [TypeOf - different values found for attribute 'TypeOf.unitPrice': " + product.getUnitPrice() + " and " + unitPrice + "." );
					logger.warn("Data consistency problem for [TypeOf - different values found for attribute 'TypeOf.unitPrice': " + product.getUnitPrice() + " and " + unitPrice + "." );
				}
				if(unitPrice != null)
					product.setUnitPrice(unitPrice);
				Integer unitsInStock = Util.getIntegerValue(r.getAs("A_unitsInStock"));
				if (product.getUnitsInStock() != null && unitsInStock != null && !product.getUnitsInStock().equals(unitsInStock)) {
					res.addLogEvent("Data consistency problem for [TypeOf - different values found for attribute 'TypeOf.unitsInStock': " + product.getUnitsInStock() + " and " + unitsInStock + "." );
					logger.warn("Data consistency problem for [TypeOf - different values found for attribute 'TypeOf.unitsInStock': " + product.getUnitsInStock() + " and " + unitsInStock + "." );
				}
				if(unitsInStock != null)
					product.setUnitsInStock(unitsInStock);
				Integer unitsOnOrder = Util.getIntegerValue(r.getAs("A_unitsOnOrder"));
				if (product.getUnitsOnOrder() != null && unitsOnOrder != null && !product.getUnitsOnOrder().equals(unitsOnOrder)) {
					res.addLogEvent("Data consistency problem for [TypeOf - different values found for attribute 'TypeOf.unitsOnOrder': " + product.getUnitsOnOrder() + " and " + unitsOnOrder + "." );
					logger.warn("Data consistency problem for [TypeOf - different values found for attribute 'TypeOf.unitsOnOrder': " + product.getUnitsOnOrder() + " and " + unitsOnOrder + "." );
				}
				if(unitsOnOrder != null)
					product.setUnitsOnOrder(unitsOnOrder);
				Integer reorderLevel = Util.getIntegerValue(r.getAs("A_reorderLevel"));
				if (product.getReorderLevel() != null && reorderLevel != null && !product.getReorderLevel().equals(reorderLevel)) {
					res.addLogEvent("Data consistency problem for [TypeOf - different values found for attribute 'TypeOf.reorderLevel': " + product.getReorderLevel() + " and " + reorderLevel + "." );
					logger.warn("Data consistency problem for [TypeOf - different values found for attribute 'TypeOf.reorderLevel': " + product.getReorderLevel() + " and " + reorderLevel + "." );
				}
				if(reorderLevel != null)
					product.setReorderLevel(reorderLevel);
				Boolean discontinued = Util.getBooleanValue(r.getAs("A_discontinued"));
				if (product.getDiscontinued() != null && discontinued != null && !product.getDiscontinued().equals(discontinued)) {
					res.addLogEvent("Data consistency problem for [TypeOf - different values found for attribute 'TypeOf.discontinued': " + product.getDiscontinued() + " and " + discontinued + "." );
					logger.warn("Data consistency problem for [TypeOf - different values found for attribute 'TypeOf.discontinued': " + product.getDiscontinued() + " and " + discontinued + "." );
				}
				if(discontinued != null)
					product.setDiscontinued(discontinued);
	
				o = r.getAs("category");
				Categories category = new Categories();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						category.setCategoryID(Util.getIntegerValue(r2.getAs("categoryID")));
						category.setCategoryName(Util.getStringValue(r2.getAs("categoryName")));
						category.setDescription(Util.getStringValue(r2.getAs("description")));
						category.setPicture(Util.getByteArrayValue(r2.getAs("picture")));
					} 
					if(o instanceof Categories) {
						category = (Categories) o;
					}
				}
	
				res.setCategory(category);
	
				return res;
		}, Encoders.bean(TypeOf.class));
	
		
		
	}
	public static Dataset<TypeOf> fullLeftOuterJoinBetweenTypeOfAndCategory(Dataset<TypeOf> d1, Dataset<Categories> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("categoryID", "A_categoryID")
			.withColumnRenamed("categoryName", "A_categoryName")
			.withColumnRenamed("description", "A_description")
			.withColumnRenamed("picture", "A_picture")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("category.categoryID").equalTo(d2_.col("A_categoryID"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, TypeOf>) r -> {
				TypeOf res = new TypeOf();
	
				Categories category = new Categories();
				Object o = r.getAs("category");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						category.setCategoryID(Util.getIntegerValue(r2.getAs("categoryID")));
						category.setCategoryName(Util.getStringValue(r2.getAs("categoryName")));
						category.setDescription(Util.getStringValue(r2.getAs("description")));
						category.setPicture(Util.getByteArrayValue(r2.getAs("picture")));
					} 
					if(o instanceof Categories) {
						category = (Categories) o;
					}
				}
	
				res.setCategory(category);
	
				Integer categoryID = Util.getIntegerValue(r.getAs("A_categoryID"));
				if (category.getCategoryID() != null && categoryID != null && !category.getCategoryID().equals(categoryID)) {
					res.addLogEvent("Data consistency problem for [TypeOf - different values found for attribute 'TypeOf.categoryID': " + category.getCategoryID() + " and " + categoryID + "." );
					logger.warn("Data consistency problem for [TypeOf - different values found for attribute 'TypeOf.categoryID': " + category.getCategoryID() + " and " + categoryID + "." );
				}
				if(categoryID != null)
					category.setCategoryID(categoryID);
				String categoryName = Util.getStringValue(r.getAs("A_categoryName"));
				if (category.getCategoryName() != null && categoryName != null && !category.getCategoryName().equals(categoryName)) {
					res.addLogEvent("Data consistency problem for [TypeOf - different values found for attribute 'TypeOf.categoryName': " + category.getCategoryName() + " and " + categoryName + "." );
					logger.warn("Data consistency problem for [TypeOf - different values found for attribute 'TypeOf.categoryName': " + category.getCategoryName() + " and " + categoryName + "." );
				}
				if(categoryName != null)
					category.setCategoryName(categoryName);
				String description = Util.getStringValue(r.getAs("A_description"));
				if (category.getDescription() != null && description != null && !category.getDescription().equals(description)) {
					res.addLogEvent("Data consistency problem for [TypeOf - different values found for attribute 'TypeOf.description': " + category.getDescription() + " and " + description + "." );
					logger.warn("Data consistency problem for [TypeOf - different values found for attribute 'TypeOf.description': " + category.getDescription() + " and " + description + "." );
				}
				if(description != null)
					category.setDescription(description);
				byte[] picture = Util.getByteArrayValue(r.getAs("A_picture"));
				if (category.getPicture() != null && picture != null && !category.getPicture().equals(picture)) {
					res.addLogEvent("Data consistency problem for [TypeOf - different values found for attribute 'TypeOf.picture': " + category.getPicture() + " and " + picture + "." );
					logger.warn("Data consistency problem for [TypeOf - different values found for attribute 'TypeOf.picture': " + category.getPicture() + " and " + picture + "." );
				}
				if(picture != null)
					category.setPicture(picture);
	
				o = r.getAs("product");
				Products product = new Products();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						product.setProductId(Util.getIntegerValue(r2.getAs("productId")));
						product.setProductName(Util.getStringValue(r2.getAs("productName")));
						product.setQuantityPerUnit(Util.getStringValue(r2.getAs("quantityPerUnit")));
						product.setUnitPrice(Util.getDoubleValue(r2.getAs("unitPrice")));
						product.setUnitsInStock(Util.getIntegerValue(r2.getAs("unitsInStock")));
						product.setUnitsOnOrder(Util.getIntegerValue(r2.getAs("unitsOnOrder")));
						product.setReorderLevel(Util.getIntegerValue(r2.getAs("reorderLevel")));
						product.setDiscontinued(Util.getBooleanValue(r2.getAs("discontinued")));
					} 
					if(o instanceof Products) {
						product = (Products) o;
					}
				}
	
				res.setProduct(product);
	
				return res;
		}, Encoders.bean(TypeOf.class));
	
		
		
	}
	
	public static Dataset<TypeOf> fullOuterJoinsTypeOf(List<Dataset<TypeOf>> datasetsPOJO) {
		return fullOuterJoinsTypeOf(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<TypeOf> fullLeftOuterJoinsTypeOf(List<Dataset<TypeOf>> datasetsPOJO) {
		return fullOuterJoinsTypeOf(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<TypeOf> fullOuterJoinsTypeOf(List<Dataset<TypeOf>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		List<String> idFields = new ArrayList<String>();
		idFields.add("product.productId");
	
		idFields.add("category.categoryID");
		scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
		
		List<Dataset<Row>> rows = new ArrayList<Dataset<Row>>();
		for(int i = 0; i < datasetsPOJO.size(); i++) {
			Dataset<TypeOf> d = datasetsPOJO.get(i);
			rows.add(d
				.withColumn("product_productId_" + i, d.col("product.productId"))
				.withColumn("category_categoryID_" + i, d.col("category.categoryID"))
				.withColumnRenamed("product", "product_" + i)
				.withColumnRenamed("category", "category_" + i)
				.withColumnRenamed("logEvents", "logEvents_" + i));
		}
		
		Column joinCond;
		joinCond = rows.get(0).col("product_productId_0").equalTo(rows.get(1).col("product_productId_1"));
		joinCond = joinCond.and(rows.get(0).col("category_categoryID_0").equalTo(rows.get(1).col("category_categoryID_1")));
		
		Dataset<Row> res = rows.get(0).join(rows.get(1), joinCond, joinMode);
		for(int i = 2; i < rows.size(); i++) {
			joinCond = rows.get(i - 1).col("product_productId_" + (i - 1)).equalTo(rows.get(i).col("product_productId_" + i));
			joinCond = joinCond.and(rows.get(i - 1).col("category_categoryID_" + (i - 1)).equalTo(rows.get(i).col("category_categoryID_" + i)));
			res = res.join(rows.get(i), joinCond, joinMode);
		}
	
		return res.map((MapFunction<Row, TypeOf>) r -> {
				TypeOf typeOf_res = new TypeOf();
	
					WrappedArray logEvents = r.getAs("logEvents_0");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							typeOf_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							typeOf_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					Products product_res = new Products();
					Categories category_res = new Categories();
					
					// attribute 'Products.productId'
					Integer firstNotNull_product_productId = Util.getIntegerValue(r.getAs("product_0.productId"));
					product_res.setProductId(firstNotNull_product_productId);
					// attribute 'Products.productName'
					String firstNotNull_product_ProductName = Util.getStringValue(r.getAs("product_0.productName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String product_productName2 = Util.getStringValue(r.getAs("product_" + i + ".productName"));
						if (firstNotNull_product_ProductName != null && product_productName2 != null && !firstNotNull_product_ProductName.equals(product_productName2)) {
							typeOf_res.addLogEvent("Data consistency problem for [Products - id :"+product_res.getProductId()+"]: different values found for attribute 'Products.productName': " + firstNotNull_product_ProductName + " and " + product_productName2 + "." );
							logger.warn("Data consistency problem for [Products - id :"+product_res.getProductId()+"]: different values found for attribute 'Products.productName': " + firstNotNull_product_ProductName + " and " + product_productName2 + "." );
						}
						if (firstNotNull_product_ProductName == null && product_productName2 != null) {
							firstNotNull_product_ProductName = product_productName2;
						}
					}
					product_res.setProductName(firstNotNull_product_ProductName);
					// attribute 'Products.quantityPerUnit'
					String firstNotNull_product_QuantityPerUnit = Util.getStringValue(r.getAs("product_0.quantityPerUnit"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String product_quantityPerUnit2 = Util.getStringValue(r.getAs("product_" + i + ".quantityPerUnit"));
						if (firstNotNull_product_QuantityPerUnit != null && product_quantityPerUnit2 != null && !firstNotNull_product_QuantityPerUnit.equals(product_quantityPerUnit2)) {
							typeOf_res.addLogEvent("Data consistency problem for [Products - id :"+product_res.getProductId()+"]: different values found for attribute 'Products.quantityPerUnit': " + firstNotNull_product_QuantityPerUnit + " and " + product_quantityPerUnit2 + "." );
							logger.warn("Data consistency problem for [Products - id :"+product_res.getProductId()+"]: different values found for attribute 'Products.quantityPerUnit': " + firstNotNull_product_QuantityPerUnit + " and " + product_quantityPerUnit2 + "." );
						}
						if (firstNotNull_product_QuantityPerUnit == null && product_quantityPerUnit2 != null) {
							firstNotNull_product_QuantityPerUnit = product_quantityPerUnit2;
						}
					}
					product_res.setQuantityPerUnit(firstNotNull_product_QuantityPerUnit);
					// attribute 'Products.unitPrice'
					Double firstNotNull_product_UnitPrice = Util.getDoubleValue(r.getAs("product_0.unitPrice"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double product_unitPrice2 = Util.getDoubleValue(r.getAs("product_" + i + ".unitPrice"));
						if (firstNotNull_product_UnitPrice != null && product_unitPrice2 != null && !firstNotNull_product_UnitPrice.equals(product_unitPrice2)) {
							typeOf_res.addLogEvent("Data consistency problem for [Products - id :"+product_res.getProductId()+"]: different values found for attribute 'Products.unitPrice': " + firstNotNull_product_UnitPrice + " and " + product_unitPrice2 + "." );
							logger.warn("Data consistency problem for [Products - id :"+product_res.getProductId()+"]: different values found for attribute 'Products.unitPrice': " + firstNotNull_product_UnitPrice + " and " + product_unitPrice2 + "." );
						}
						if (firstNotNull_product_UnitPrice == null && product_unitPrice2 != null) {
							firstNotNull_product_UnitPrice = product_unitPrice2;
						}
					}
					product_res.setUnitPrice(firstNotNull_product_UnitPrice);
					// attribute 'Products.unitsInStock'
					Integer firstNotNull_product_UnitsInStock = Util.getIntegerValue(r.getAs("product_0.unitsInStock"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer product_unitsInStock2 = Util.getIntegerValue(r.getAs("product_" + i + ".unitsInStock"));
						if (firstNotNull_product_UnitsInStock != null && product_unitsInStock2 != null && !firstNotNull_product_UnitsInStock.equals(product_unitsInStock2)) {
							typeOf_res.addLogEvent("Data consistency problem for [Products - id :"+product_res.getProductId()+"]: different values found for attribute 'Products.unitsInStock': " + firstNotNull_product_UnitsInStock + " and " + product_unitsInStock2 + "." );
							logger.warn("Data consistency problem for [Products - id :"+product_res.getProductId()+"]: different values found for attribute 'Products.unitsInStock': " + firstNotNull_product_UnitsInStock + " and " + product_unitsInStock2 + "." );
						}
						if (firstNotNull_product_UnitsInStock == null && product_unitsInStock2 != null) {
							firstNotNull_product_UnitsInStock = product_unitsInStock2;
						}
					}
					product_res.setUnitsInStock(firstNotNull_product_UnitsInStock);
					// attribute 'Products.unitsOnOrder'
					Integer firstNotNull_product_UnitsOnOrder = Util.getIntegerValue(r.getAs("product_0.unitsOnOrder"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer product_unitsOnOrder2 = Util.getIntegerValue(r.getAs("product_" + i + ".unitsOnOrder"));
						if (firstNotNull_product_UnitsOnOrder != null && product_unitsOnOrder2 != null && !firstNotNull_product_UnitsOnOrder.equals(product_unitsOnOrder2)) {
							typeOf_res.addLogEvent("Data consistency problem for [Products - id :"+product_res.getProductId()+"]: different values found for attribute 'Products.unitsOnOrder': " + firstNotNull_product_UnitsOnOrder + " and " + product_unitsOnOrder2 + "." );
							logger.warn("Data consistency problem for [Products - id :"+product_res.getProductId()+"]: different values found for attribute 'Products.unitsOnOrder': " + firstNotNull_product_UnitsOnOrder + " and " + product_unitsOnOrder2 + "." );
						}
						if (firstNotNull_product_UnitsOnOrder == null && product_unitsOnOrder2 != null) {
							firstNotNull_product_UnitsOnOrder = product_unitsOnOrder2;
						}
					}
					product_res.setUnitsOnOrder(firstNotNull_product_UnitsOnOrder);
					// attribute 'Products.reorderLevel'
					Integer firstNotNull_product_ReorderLevel = Util.getIntegerValue(r.getAs("product_0.reorderLevel"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer product_reorderLevel2 = Util.getIntegerValue(r.getAs("product_" + i + ".reorderLevel"));
						if (firstNotNull_product_ReorderLevel != null && product_reorderLevel2 != null && !firstNotNull_product_ReorderLevel.equals(product_reorderLevel2)) {
							typeOf_res.addLogEvent("Data consistency problem for [Products - id :"+product_res.getProductId()+"]: different values found for attribute 'Products.reorderLevel': " + firstNotNull_product_ReorderLevel + " and " + product_reorderLevel2 + "." );
							logger.warn("Data consistency problem for [Products - id :"+product_res.getProductId()+"]: different values found for attribute 'Products.reorderLevel': " + firstNotNull_product_ReorderLevel + " and " + product_reorderLevel2 + "." );
						}
						if (firstNotNull_product_ReorderLevel == null && product_reorderLevel2 != null) {
							firstNotNull_product_ReorderLevel = product_reorderLevel2;
						}
					}
					product_res.setReorderLevel(firstNotNull_product_ReorderLevel);
					// attribute 'Products.discontinued'
					Boolean firstNotNull_product_Discontinued = Util.getBooleanValue(r.getAs("product_0.discontinued"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Boolean product_discontinued2 = Util.getBooleanValue(r.getAs("product_" + i + ".discontinued"));
						if (firstNotNull_product_Discontinued != null && product_discontinued2 != null && !firstNotNull_product_Discontinued.equals(product_discontinued2)) {
							typeOf_res.addLogEvent("Data consistency problem for [Products - id :"+product_res.getProductId()+"]: different values found for attribute 'Products.discontinued': " + firstNotNull_product_Discontinued + " and " + product_discontinued2 + "." );
							logger.warn("Data consistency problem for [Products - id :"+product_res.getProductId()+"]: different values found for attribute 'Products.discontinued': " + firstNotNull_product_Discontinued + " and " + product_discontinued2 + "." );
						}
						if (firstNotNull_product_Discontinued == null && product_discontinued2 != null) {
							firstNotNull_product_Discontinued = product_discontinued2;
						}
					}
					product_res.setDiscontinued(firstNotNull_product_Discontinued);
					// attribute 'Categories.categoryID'
					Integer firstNotNull_category_categoryID = Util.getIntegerValue(r.getAs("category_0.categoryID"));
					category_res.setCategoryID(firstNotNull_category_categoryID);
					// attribute 'Categories.categoryName'
					String firstNotNull_category_CategoryName = Util.getStringValue(r.getAs("category_0.categoryName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String category_categoryName2 = Util.getStringValue(r.getAs("category_" + i + ".categoryName"));
						if (firstNotNull_category_CategoryName != null && category_categoryName2 != null && !firstNotNull_category_CategoryName.equals(category_categoryName2)) {
							typeOf_res.addLogEvent("Data consistency problem for [Categories - id :"+category_res.getCategoryID()+"]: different values found for attribute 'Categories.categoryName': " + firstNotNull_category_CategoryName + " and " + category_categoryName2 + "." );
							logger.warn("Data consistency problem for [Categories - id :"+category_res.getCategoryID()+"]: different values found for attribute 'Categories.categoryName': " + firstNotNull_category_CategoryName + " and " + category_categoryName2 + "." );
						}
						if (firstNotNull_category_CategoryName == null && category_categoryName2 != null) {
							firstNotNull_category_CategoryName = category_categoryName2;
						}
					}
					category_res.setCategoryName(firstNotNull_category_CategoryName);
					// attribute 'Categories.description'
					String firstNotNull_category_Description = Util.getStringValue(r.getAs("category_0.description"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String category_description2 = Util.getStringValue(r.getAs("category_" + i + ".description"));
						if (firstNotNull_category_Description != null && category_description2 != null && !firstNotNull_category_Description.equals(category_description2)) {
							typeOf_res.addLogEvent("Data consistency problem for [Categories - id :"+category_res.getCategoryID()+"]: different values found for attribute 'Categories.description': " + firstNotNull_category_Description + " and " + category_description2 + "." );
							logger.warn("Data consistency problem for [Categories - id :"+category_res.getCategoryID()+"]: different values found for attribute 'Categories.description': " + firstNotNull_category_Description + " and " + category_description2 + "." );
						}
						if (firstNotNull_category_Description == null && category_description2 != null) {
							firstNotNull_category_Description = category_description2;
						}
					}
					category_res.setDescription(firstNotNull_category_Description);
					// attribute 'Categories.picture'
					byte[] firstNotNull_category_Picture = Util.getByteArrayValue(r.getAs("category_0.picture"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						byte[] category_picture2 = Util.getByteArrayValue(r.getAs("category_" + i + ".picture"));
						if (firstNotNull_category_Picture != null && category_picture2 != null && !firstNotNull_category_Picture.equals(category_picture2)) {
							typeOf_res.addLogEvent("Data consistency problem for [Categories - id :"+category_res.getCategoryID()+"]: different values found for attribute 'Categories.picture': " + firstNotNull_category_Picture + " and " + category_picture2 + "." );
							logger.warn("Data consistency problem for [Categories - id :"+category_res.getCategoryID()+"]: different values found for attribute 'Categories.picture': " + firstNotNull_category_Picture + " and " + category_picture2 + "." );
						}
						if (firstNotNull_category_Picture == null && category_picture2 != null) {
							firstNotNull_category_Picture = category_picture2;
						}
					}
					category_res.setPicture(firstNotNull_category_Picture);
	
					typeOf_res.setProduct(product_res);
					typeOf_res.setCategory(category_res);
					return typeOf_res;
		}
		, Encoders.bean(TypeOf.class));
	
	}
	
	//Empty arguments
	public Dataset<TypeOf> getTypeOfList(){
		 return getTypeOfList(null,null);
	}
	
	public abstract Dataset<TypeOf> getTypeOfList(
		Condition<ProductsAttribute> product_condition,
		Condition<CategoriesAttribute> category_condition);
	
	public Dataset<TypeOf> getTypeOfListByProductCondition(
		Condition<ProductsAttribute> product_condition
	){
		return getTypeOfList(product_condition, null);
	}
	
	public TypeOf getTypeOfByProduct(Products product) {
		Condition<ProductsAttribute> cond = null;
		cond = Condition.simple(ProductsAttribute.productId, Operator.EQUALS, product.getProductId());
		Dataset<TypeOf> res = getTypeOfListByProductCondition(cond);
		List<TypeOf> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	public Dataset<TypeOf> getTypeOfListByCategoryCondition(
		Condition<CategoriesAttribute> category_condition
	){
		return getTypeOfList(null, category_condition);
	}
	
	public Dataset<TypeOf> getTypeOfListByCategory(Categories category) {
		Condition<CategoriesAttribute> cond = null;
		cond = Condition.simple(CategoriesAttribute.categoryID, Operator.EQUALS, category.getCategoryID());
		Dataset<TypeOf> res = getTypeOfListByCategoryCondition(cond);
	return res;
	}
	
	public abstract void insertTypeOf(TypeOf typeOf);
	
	
	
	public 	abstract boolean insertTypeOfInRefStructProductsInMyRelDB(TypeOf typeOf);
	
	 public void insertTypeOf(Products product ,Categories category ){
		TypeOf typeOf = new TypeOf();
		typeOf.setProduct(product);
		typeOf.setCategory(category);
		insertTypeOf(typeOf);
	}
	
	 public void insertTypeOf(Products products, List<Categories> categoryList){
		TypeOf typeOf = new TypeOf();
		typeOf.setProduct(products);
		for(Categories category : categoryList){
			typeOf.setCategory(category);
			insertTypeOf(typeOf);
		}
	}
	
	
	public abstract void deleteTypeOfList(
		conditions.Condition<conditions.ProductsAttribute> product_condition,
		conditions.Condition<conditions.CategoriesAttribute> category_condition);
	
	public void deleteTypeOfListByProductCondition(
		conditions.Condition<conditions.ProductsAttribute> product_condition
	){
		deleteTypeOfList(product_condition, null);
	}
	
	public void deleteTypeOfByProduct(pojo.Products product) {
		// TODO using id for selecting
		return;
	}
	public void deleteTypeOfListByCategoryCondition(
		conditions.Condition<conditions.CategoriesAttribute> category_condition
	){
		deleteTypeOfList(null, category_condition);
	}
	
	public void deleteTypeOfListByCategory(pojo.Categories category) {
		// TODO using id for selecting
		return;
	}
		
}
