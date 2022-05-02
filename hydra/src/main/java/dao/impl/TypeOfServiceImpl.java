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
import conditions.TypeOfAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.ProductsTDO;
import tdo.TypeOfTDO;
import conditions.ProductsAttribute;
import dao.services.ProductsService;
import tdo.CategoriesTDO;
import tdo.TypeOfTDO;
import conditions.CategoriesAttribute;
import dao.services.CategoriesService;
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

public class TypeOfServiceImpl extends dao.services.TypeOfService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TypeOfServiceImpl.class);
	
	public  Dataset<TypeOf> getProductAndCategoryListInIsCategoryInProductsFromMyRelDB(Condition<ProductsAttribute> product_cond, Condition<CategoriesAttribute> category_cond, MutableBoolean product_refilter, MutableBoolean category_refilter) {
		Pair<String, List<String>> whereClause1 = ProductsServiceImpl.getSQLWhereClauseInProductsFromMyRelDBWithTableAlias(product_cond, product_refilter, "ProductsA.");
		Pair<String, List<String>> whereClause2 = CategoriesServiceImpl.getSQLWhereClauseInCategoriesFromMyRelDBWithTableAlias(category_cond, category_refilter, "CategoriesB.");
		
		String where1 = whereClause1.getKey();
		List<String> preparedValues = whereClause1.getValue();
		for(String preparedValue : preparedValues) {
			where1 = where1.replaceFirst("\\?", preparedValue);
		}
	
		String where2 = whereClause2.getKey();
		preparedValues = whereClause2.getValue();
		for(String preparedValue : preparedValues) {
			where2 = where2.replaceFirst("\\?", preparedValue);
		}
	
		String where = (where1 == null && where2 == null) ? "" : " AND " + (where1 != null && where2 != null ? where1 + " AND " + where2 : (where1 != null ? where1 : where2) );
		String aliasedColumns = "ProductsA.ProductID as ProductsA_ProductID,ProductsA.ProductName as ProductsA_ProductName,ProductsA.QuantityPerUnit as ProductsA_QuantityPerUnit,ProductsA.UnitPrice as ProductsA_UnitPrice,ProductsA.UnitsInStock as ProductsA_UnitsInStock,ProductsA.UnitsOnOrder as ProductsA_UnitsOnOrder,ProductsA.ReorderLevel as ProductsA_ReorderLevel,ProductsA.Discontinued as ProductsA_Discontinued,ProductsA.SupplierRef as ProductsA_SupplierRef,ProductsA.CategoryRef as ProductsA_CategoryRef,CategoriesB.CategoryID as CategoriesB_CategoryID,CategoriesB.CategoryName as CategoriesB_CategoryName,CategoriesB.Description as CategoriesB_Description,CategoriesB.Picture as CategoriesB_Picture";
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("myRelDB", "(SELECT " + aliasedColumns + " FROM Products ProductsA, Categories CategoriesB WHERE ProductsA.CategoryRef = CategoriesB.CategoryID" + where + ") AS JOIN_TABLE", null);
		
		Dataset<TypeOf> res = d.map((MapFunction<Row, TypeOf>) r -> {
					TypeOf typeOf_res = new TypeOf();
					typeOf_res.setProduct(new Products());
					typeOf_res.setCategory(new Categories());
					
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
	
	
					
					// attribute [Products.ProductId]
					Integer product_productId = Util.getIntegerValue(r.getAs("ProductsA_ProductID"));
					typeOf_res.getProduct().setProductId(product_productId);
					
					// attribute [Products.ProductName]
					String product_productName = Util.getStringValue(r.getAs("ProductsA_ProductName"));
					typeOf_res.getProduct().setProductName(product_productName);
					
					// attribute [Products.QuantityPerUnit]
					String product_quantityPerUnit = Util.getStringValue(r.getAs("ProductsA_QuantityPerUnit"));
					typeOf_res.getProduct().setQuantityPerUnit(product_quantityPerUnit);
					
					// attribute [Products.UnitPrice]
					Double product_unitPrice = Util.getDoubleValue(r.getAs("ProductsA_UnitPrice"));
					typeOf_res.getProduct().setUnitPrice(product_unitPrice);
					
					// attribute [Products.UnitsInStock]
					Integer product_unitsInStock = Util.getIntegerValue(r.getAs("ProductsA_UnitsInStock"));
					typeOf_res.getProduct().setUnitsInStock(product_unitsInStock);
					
					// attribute [Products.UnitsOnOrder]
					Integer product_unitsOnOrder = Util.getIntegerValue(r.getAs("ProductsA_UnitsOnOrder"));
					typeOf_res.getProduct().setUnitsOnOrder(product_unitsOnOrder);
					
					// attribute [Products.ReorderLevel]
					Integer product_reorderLevel = Util.getIntegerValue(r.getAs("ProductsA_ReorderLevel"));
					typeOf_res.getProduct().setReorderLevel(product_reorderLevel);
					
					// attribute [Products.Discontinued]
					Boolean product_discontinued = Util.getBooleanValue(r.getAs("ProductsA_Discontinued"));
					typeOf_res.getProduct().setDiscontinued(product_discontinued);
	
					
					// attribute [Category.CategoryID]
					Integer category_categoryID = Util.getIntegerValue(r.getAs("CategoriesB_CategoryID"));
					typeOf_res.getCategory().setCategoryID(category_categoryID);
					
					// attribute [Category.CategoryName]
					String category_categoryName = Util.getStringValue(r.getAs("CategoriesB_CategoryName"));
					typeOf_res.getCategory().setCategoryName(category_categoryName);
					
					// attribute [Category.Description]
					String category_description = Util.getStringValue(r.getAs("CategoriesB_Description"));
					typeOf_res.getCategory().setDescription(category_description);
					
					// attribute [Category.Picture]
					byte[] category_picture = Util.getByteArrayValue(r.getAs("CategoriesB_Picture"));
					typeOf_res.getCategory().setPicture(category_picture);
	
					return typeOf_res;
				}, Encoders.bean(TypeOf.class));
	
		return res;
	}
	
	
	
	
	
	public Dataset<TypeOf> getTypeOfList(
		Condition<ProductsAttribute> product_condition,
		Condition<CategoriesAttribute> category_condition){
			TypeOfServiceImpl typeOfService = this;
			ProductsService productsService = new ProductsServiceImpl();  
			CategoriesService categoriesService = new CategoriesServiceImpl();
			MutableBoolean product_refilter = new MutableBoolean(false);
			List<Dataset<TypeOf>> datasetsPOJO = new ArrayList<Dataset<TypeOf>>();
			boolean all_already_persisted = false;
			MutableBoolean category_refilter = new MutableBoolean(false);
			
			org.apache.spark.sql.Column joinCondition = null;
			// For role 'product' in reference 'isCategory'. A->B Scenario in SQL DB
			category_refilter = new MutableBoolean(false);
			Dataset<TypeOf> res_isCategory = typeOfService.getProductAndCategoryListInIsCategoryInProductsFromMyRelDB(product_condition, category_condition, product_refilter, category_refilter);
			datasetsPOJO.add(res_isCategory);
		
			
			Dataset<TypeOf> res_typeOf_product;
			Dataset<Products> res_Products;
			
			
			//Join datasets or return 
			Dataset<TypeOf> res = fullOuterJoinsTypeOf(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Products> lonelyProduct = null;
			Dataset<Categories> lonelyCategory = null;
			
		
		
			
			if(product_refilter.booleanValue() || category_refilter.booleanValue())
				res = res.filter((FilterFunction<TypeOf>) r -> (product_condition == null || product_condition.evaluate(r.getProduct())) && (category_condition == null || category_condition.evaluate(r.getCategory())));
			
		
			return res;
		
		}
	
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
	
	public void insertTypeOf(TypeOf typeOf){
		//Link entities in join structures.
		// Update embedded structures mapped to non mandatory roles.
		// Update ref fields mapped to non mandatory roles. 
		insertTypeOfInRefStructProductsInMyRelDB(typeOf);
	}
	
	
	
	public 	boolean insertTypeOfInRefStructProductsInMyRelDB(TypeOf typeOf){
	 	// Rel 'typeOf' Insert in reference structure 'Products'
		Products productsProduct = typeOf.getProduct();
		Categories categoriesCategory = typeOf.getCategory();
	
		List<String> columns = new ArrayList<>();
		List<Object> values = new ArrayList<>();
		String filtercolumn;
		Object filtervalue;
		columns.add("CategoryRef");
		values.add(categoriesCategory==null?null:categoriesCategory.getCategoryID());
		filtercolumn = "ProductID";
		filtervalue = productsProduct.getProductId();
		DBConnectionMgr.updateInTable(filtercolumn, filtervalue, columns, values, "Products", "myRelDB");					
		return true;
	}
	
	
	
	
	public void deleteTypeOfList(
		conditions.Condition<conditions.ProductsAttribute> product_condition,
		conditions.Condition<conditions.CategoriesAttribute> category_condition){
			//TODO
		}
	
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
