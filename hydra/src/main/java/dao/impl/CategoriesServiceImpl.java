package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Categories;
import conditions.*;
import dao.services.CategoriesService;
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


public class CategoriesServiceImpl extends CategoriesService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CategoriesServiceImpl.class);
	
	
	
	
	public static Pair<String, List<String>> getSQLWhereClauseInCategoriesFromMyRelDB(Condition<CategoriesAttribute> condition, MutableBoolean refilterFlag) {
		return getSQLWhereClauseInCategoriesFromMyRelDBWithTableAlias(condition, refilterFlag, "");
	}
	
	public static List<String> getSQLSetClauseInCategoriesFromMyRelDB(conditions.SetClause<CategoriesAttribute> set) {
		List<String> res = new ArrayList<String>();
		if(set != null) {
			java.util.Map<String, java.util.Map<String, String>> longFieldValues = new java.util.HashMap<String, java.util.Map<String, String>>();
			java.util.Map<CategoriesAttribute, Object> clause = set.getClause();
			for(java.util.Map.Entry<CategoriesAttribute, Object> e : clause.entrySet()) {
				CategoriesAttribute attr = e.getKey();
				Object value = e.getValue();
				if(attr == CategoriesAttribute.categoryID ) {
					res.add("CategoryID = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == CategoriesAttribute.categoryName ) {
					res.add("CategoryName = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == CategoriesAttribute.description ) {
					res.add("Description = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == CategoriesAttribute.picture ) {
					res.add("Picture = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
			}
	
			for(java.util.Map.Entry<String, java.util.Map<String, String>> entry : longFieldValues.entrySet()) {
				String longField = entry.getKey();
				java.util.Map<String, String> values = entry.getValue();
			}
	
		}
		return res;
	}
	
	public static Pair<String, List<String>> getSQLWhereClauseInCategoriesFromMyRelDBWithTableAlias(Condition<CategoriesAttribute> condition, MutableBoolean refilterFlag, String tableAlias) {
		String where = null;	
		List<String> preparedValues = new java.util.ArrayList<String>();
		if(condition != null) {
			
			if(condition instanceof SimpleCondition) {
				CategoriesAttribute attr = ((SimpleCondition<CategoriesAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<CategoriesAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<CategoriesAttribute>) condition).getValue();
				if(value != null) {
					boolean isConditionAttrEncountered = false;
					if(attr == CategoriesAttribute.categoryID ) {
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
						
						where = tableAlias + "CategoryID " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == CategoriesAttribute.categoryName ) {
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
						
						where = tableAlias + "CategoryName " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == CategoriesAttribute.description ) {
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
						
						where = tableAlias + "Description " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == CategoriesAttribute.picture ) {
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
						
						where = tableAlias + "Picture " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						where = "1 = 1";
					}
				} else {
					if(attr == CategoriesAttribute.categoryID ) {
						if(op == Operator.EQUALS)
							where =  "CategoryID IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "CategoryID IS NOT NULL";
					}
					if(attr == CategoriesAttribute.categoryName ) {
						if(op == Operator.EQUALS)
							where =  "CategoryName IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "CategoryName IS NOT NULL";
					}
					if(attr == CategoriesAttribute.description ) {
						if(op == Operator.EQUALS)
							where =  "Description IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "Description IS NOT NULL";
					}
					if(attr == CategoriesAttribute.picture ) {
						if(op == Operator.EQUALS)
							where =  "Picture IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "Picture IS NOT NULL";
					}
				}
			}
	
			if(condition instanceof AndCondition) {
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInCategoriesFromMyRelDB(((AndCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInCategoriesFromMyRelDB(((AndCondition) condition).getRightCondition(), refilterFlag);
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
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInCategoriesFromMyRelDB(((OrCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInCategoriesFromMyRelDB(((OrCondition) condition).getRightCondition(), refilterFlag);
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
	
	
	
	public Dataset<Categories> getCategoriesListInCategoriesFromMyRelDB(conditions.Condition<conditions.CategoriesAttribute> condition, MutableBoolean refilterFlag){
	
		Pair<String, List<String>> whereClause = CategoriesServiceImpl.getSQLWhereClauseInCategoriesFromMyRelDB(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", preparedValue);
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("myRelDB", "Categories", where);
		
	
		Dataset<Categories> res = d.map((MapFunction<Row, Categories>) r -> {
					Categories categories_res = new Categories();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
					// attribute [Categories.CategoryID]
					Integer categoryID = Util.getIntegerValue(r.getAs("CategoryID"));
					categories_res.setCategoryID(categoryID);
					
					// attribute [Categories.CategoryName]
					String categoryName = Util.getStringValue(r.getAs("CategoryName"));
					categories_res.setCategoryName(categoryName);
					
					// attribute [Categories.Description]
					String description = Util.getStringValue(r.getAs("Description"));
					categories_res.setDescription(description);
					
					// attribute [Categories.Picture]
					byte[] picture = Util.getByteArrayValue(r.getAs("Picture"));
					categories_res.setPicture(picture);
	
	
	
					return categories_res;
				}, Encoders.bean(Categories.class));
	
	
		return res;
		
	}
	
	
	
	
	
	
	public Dataset<Categories> getCategoryListInTypeOf(conditions.Condition<conditions.ProductsAttribute> product_condition,conditions.Condition<conditions.CategoriesAttribute> category_condition)		{
		MutableBoolean category_refilter = new MutableBoolean(false);
		List<Dataset<Categories>> datasetsPOJO = new ArrayList<Dataset<Categories>>();
		Dataset<Products> all = null;
		boolean all_already_persisted = false;
		MutableBoolean product_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		product_refilter = new MutableBoolean(false);
		// For role 'product' in reference 'isCategory'  B->A Scenario in SQL Db.
		Dataset<TypeOf> res_isCategory = typeOfService.getProductAndCategoryListInIsCategoryInProductsFromMyRelDB(product_condition, category_condition, product_refilter, category_refilter);
		Dataset<Categories> res_Categories_isCategory = null;
		if(product_refilter.booleanValue()) {
			if(all == null)
				all = new ProductsServiceImpl().getProductsList(product_condition);
			joinCondition = null;
			joinCondition = res_isCategory.col("product.productId").equalTo(all.col("productId"));
			if(joinCondition == null)
				res_Categories_isCategory = res_isCategory.join(all).select("category.*").as(Encoders.bean(Categories.class));
			else
				res_Categories_isCategory = res_isCategory.join(all, joinCondition).select("category.*").as(Encoders.bean(Categories.class));
		} else
			res_Categories_isCategory = res_isCategory.map((MapFunction<TypeOf,Categories>) r -> r.getCategory(), Encoders.bean(Categories.class));
		res_Categories_isCategory = res_Categories_isCategory.dropDuplicates(new String[] {"categoryID"});
		datasetsPOJO.add(res_Categories_isCategory);
		
		Dataset<TypeOf> res_typeOf_category;
		Dataset<Categories> res_Categories;
		
		
		//Join datasets or return 
		Dataset<Categories> res = fullOuterJoinsCategories(datasetsPOJO);
		if(res == null)
			return null;
	
		if(category_refilter.booleanValue())
			res = res.filter((FilterFunction<Categories>) r -> category_condition == null || category_condition.evaluate(r));
		
	
		return res;
		}
	
	
	public boolean insertCategories(Categories categories){
		// Insert into all mapped standalone AbstractPhysicalStructure 
		boolean inserted = false;
			inserted = insertCategoriesInCategoriesFromMyRelDB(categories) || inserted ;
		return inserted;
	}
	
	public boolean insertCategoriesInCategoriesFromMyRelDB(Categories categories)	{
		String idvalue="";
		idvalue+=categories.getCategoryID();
		boolean entityExists = false; // Modify in acceleo code (in 'main.services.insert.entitytype.generateSimpleInsertMethods.mtl') to generate checking before insert
		if(!entityExists){
		List<String> columns = new ArrayList<>();
		List<Object> values = new ArrayList<>();	
		columns.add("CategoryID");
		values.add(categories.getCategoryID());
		columns.add("CategoryName");
		values.add(categories.getCategoryName());
		columns.add("Description");
		values.add(categories.getDescription());
		columns.add("Picture");
		values.add(categories.getPicture());
		DBConnectionMgr.insertInTable(columns, Arrays.asList(values), "Categories", "myRelDB");
			logger.info("Inserted [Categories] entity ID [{}] in [Categories] in database [MyRelDB]", idvalue);
		}
		else
			logger.warn("[Categories] entity ID [{}] already present in [Categories] in database [MyRelDB]", idvalue);
		return !entityExists;
	} 
	
	private boolean inUpdateMethod = false;
	private List<Row> allCategoriesIdList = null;
	public void updateCategoriesList(conditions.Condition<conditions.CategoriesAttribute> condition, conditions.SetClause<conditions.CategoriesAttribute> set){
		inUpdateMethod = true;
		try {
			MutableBoolean refilterInCategoriesFromMyRelDB = new MutableBoolean(false);
			getSQLWhereClauseInCategoriesFromMyRelDB(condition, refilterInCategoriesFromMyRelDB);
			// one first updates in the structures necessitating to execute a "SELECT *" query to establish the update condition 
			if(refilterInCategoriesFromMyRelDB.booleanValue())
				updateCategoriesListInCategoriesFromMyRelDB(condition, set);
		
	
			if(!refilterInCategoriesFromMyRelDB.booleanValue())
				updateCategoriesListInCategoriesFromMyRelDB(condition, set);
	
		} finally {
			inUpdateMethod = false;
		}
	}
	
	
	public void updateCategoriesListInCategoriesFromMyRelDB(Condition<CategoriesAttribute> condition, SetClause<CategoriesAttribute> set) {
		List<String> setClause = CategoriesServiceImpl.getSQLSetClauseInCategoriesFromMyRelDB(set);
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
		Pair<String, List<String>> whereClause = CategoriesServiceImpl.getSQLWhereClauseInCategoriesFromMyRelDB(condition, refilter);
		if(!refilter.booleanValue()) {
			String where = whereClause.getKey();
			List<String> preparedValues = whereClause.getValue();
			for(String preparedValue : preparedValues) {
				where = where.replaceFirst("\\?", preparedValue);
			}
			
			String sql = "UPDATE Categories SET " + setSQL;
			if(where != null)
				sql += " WHERE " + where;
			
			DBConnectionMgr.updateInTable(sql, "myRelDB");
		} else {
			if(!inUpdateMethod || allCategoriesIdList == null)
				allCategoriesIdList = this.getCategoriesList(condition).select("categoryID").collectAsList();
		
			List<String> updateQueries = new ArrayList<String>();
			for(Row row : allCategoriesIdList) {
				Condition<CategoriesAttribute> conditionId = null;
				conditionId = Condition.simple(CategoriesAttribute.categoryID, Operator.EQUALS, row.getAs("categoryID"));
				whereClause = CategoriesServiceImpl.getSQLWhereClauseInCategoriesFromMyRelDB(conditionId, refilter);
				String sql = "UPDATE Categories SET " + setSQL;
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
	
	
	
	public void updateCategories(pojo.Categories categories) {
		//TODO using the id
		return;
	}
	public void updateCategoryListInTypeOf(
		conditions.Condition<conditions.ProductsAttribute> product_condition,
		conditions.Condition<conditions.CategoriesAttribute> category_condition,
		
		conditions.SetClause<conditions.CategoriesAttribute> set
	){
		//TODO
	}
	
	public void updateCategoryListInTypeOfByProductCondition(
		conditions.Condition<conditions.ProductsAttribute> product_condition,
		conditions.SetClause<conditions.CategoriesAttribute> set
	){
		updateCategoryListInTypeOf(product_condition, null, set);
	}
	
	public void updateCategoryInTypeOfByProduct(
		pojo.Products product,
		conditions.SetClause<conditions.CategoriesAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateCategoryListInTypeOfByCategoryCondition(
		conditions.Condition<conditions.CategoriesAttribute> category_condition,
		conditions.SetClause<conditions.CategoriesAttribute> set
	){
		updateCategoryListInTypeOf(null, category_condition, set);
	}
	
	
	public void deleteCategoriesList(conditions.Condition<conditions.CategoriesAttribute> condition){
		//TODO
	}
	
	public void deleteCategories(pojo.Categories categories) {
		//TODO using the id
		return;
	}
	public void deleteCategoryListInTypeOf(	
		conditions.Condition<conditions.ProductsAttribute> product_condition,	
		conditions.Condition<conditions.CategoriesAttribute> category_condition){
			//TODO
		}
	
	public void deleteCategoryListInTypeOfByProductCondition(
		conditions.Condition<conditions.ProductsAttribute> product_condition
	){
		deleteCategoryListInTypeOf(product_condition, null);
	}
	
	public void deleteCategoryInTypeOfByProduct(
		pojo.Products product 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteCategoryListInTypeOfByCategoryCondition(
		conditions.Condition<conditions.CategoriesAttribute> category_condition
	){
		deleteCategoryListInTypeOf(null, category_condition);
	}
	
}
