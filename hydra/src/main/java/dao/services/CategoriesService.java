package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import pojo.Categories;
import conditions.CategoriesAttribute;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.commons.lang.mutable.MutableBoolean;
import conditions.Condition;
import conditions.Operator;
import util.Util;
import conditions.CategoriesAttribute;
import pojo.TypeOf;
import conditions.ProductsAttribute;
import pojo.Products;

public abstract class CategoriesService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CategoriesService.class);
	protected TypeOfService typeOfService = new dao.impl.TypeOfServiceImpl();
	


	public static enum ROLE_NAME {
		TYPEOF_CATEGORY
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.TYPEOF_CATEGORY, loading.Loading.LAZY);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public CategoriesService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public CategoriesService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
		this();
		if(loadingParams != null)
			for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: loadingParams.entrySet())
				loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public static java.util.Map<ROLE_NAME, loading.Loading> getDefaultLoadingParameters() {
		java.util.Map<ROLE_NAME, loading.Loading> res = new java.util.HashMap<ROLE_NAME, loading.Loading>();
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
				res.put(entry.getKey(), entry.getValue());
		return res;
	}
	
	public static void setAllDefaultLoadingParameters(loading.Loading loading) {
		java.util.Map<ROLE_NAME, loading.Loading> newParams = new java.util.HashMap<ROLE_NAME, loading.Loading>();
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
				newParams.put(entry.getKey(), entry.getValue());
		defaultLoadingParameters = newParams;
	}
	
	public java.util.Map<ROLE_NAME, loading.Loading> getLoadingParameters() {
		return this.loadingParameters;
	}
	
	public void setLoadingParameters(java.util.Map<ROLE_NAME, loading.Loading> newParams) {
		this.loadingParameters = newParams;
	}
	
	public void updateLoadingParameter(ROLE_NAME role, loading.Loading l) {
		this.loadingParameters.put(role, l);
	}
	
	
	public Dataset<Categories> getCategoriesList(){
		return getCategoriesList(null);
	}
	
	public Dataset<Categories> getCategoriesList(conditions.Condition<conditions.CategoriesAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Categories>> datasets = new ArrayList<Dataset<Categories>>();
		Dataset<Categories> d = null;
		d = getCategoriesListInCategoriesFromMyRelDB(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsCategories(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Categories>) r -> condition == null || condition.evaluate(r));
		d = d.dropDuplicates(new String[] {"categoryID"});
		return d;
	}
	
	
	
	
	
	public abstract Dataset<Categories> getCategoriesListInCategoriesFromMyRelDB(conditions.Condition<conditions.CategoriesAttribute> condition, MutableBoolean refilterFlag);
	
	
	public Categories getCategoriesById(Integer categoryID){
		Condition cond;
		cond = Condition.simple(CategoriesAttribute.categoryID, conditions.Operator.EQUALS, categoryID);
		Dataset<Categories> res = getCategoriesList(cond);
		if(res!=null && !res.isEmpty())
			return res.first();
		return null;
	}
	
	public Dataset<Categories> getCategoriesListByCategoryID(Integer categoryID) {
		return getCategoriesList(conditions.Condition.simple(conditions.CategoriesAttribute.categoryID, conditions.Operator.EQUALS, categoryID));
	}
	
	public Dataset<Categories> getCategoriesListByCategoryName(String CategoryName) {
		return getCategoriesList(conditions.Condition.simple(conditions.CategoriesAttribute.CategoryName, conditions.Operator.EQUALS, CategoryName));
	}
	
	public Dataset<Categories> getCategoriesListByDescription(String Description) {
		return getCategoriesList(conditions.Condition.simple(conditions.CategoriesAttribute.Description, conditions.Operator.EQUALS, Description));
	}
	
	public Dataset<Categories> getCategoriesListByPicture(byte[] Picture) {
		return getCategoriesList(conditions.Condition.simple(conditions.CategoriesAttribute.Picture, conditions.Operator.EQUALS, Picture));
	}
	
	
	
	public static Dataset<Categories> fullOuterJoinsCategories(List<Dataset<Categories>> datasetsPOJO) {
		return fullOuterJoinsCategories(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Categories> fullLeftOuterJoinsCategories(List<Dataset<Categories>> datasetsPOJO) {
		return fullOuterJoinsCategories(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Categories> fullOuterJoinsCategories(List<Dataset<Categories>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Categories> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("categoryID");
			logger.debug("Start {} of [{}] datasets of [Categories] objects",joinMode,datasetsPOJO.size());
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("categoryName", "categoryName_1")
								.withColumnRenamed("description", "description_1")
								.withColumnRenamed("picture", "picture_1")
								.withColumnRenamed("logEvents", "logEvents_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("categoryName", "categoryName_" + i)
								.withColumnRenamed("description", "description_" + i)
								.withColumnRenamed("picture", "picture_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
						, seq, joinMode);
			}
			logger.debug("End join. Start");
			logger.debug("Start transforming Row objects to [Categories] objects"); 
			d = res.map((MapFunction<Row, Categories>) r -> {
					Categories categories_res = new Categories();
					
					// attribute 'Categories.categoryID'
					Integer firstNotNull_categoryID = Util.getIntegerValue(r.getAs("categoryID"));
					categories_res.setCategoryID(firstNotNull_categoryID);
					
					// attribute 'Categories.categoryName'
					String firstNotNull_CategoryName = Util.getStringValue(r.getAs("categoryName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String categoryName2 = Util.getStringValue(r.getAs("categoryName_" + i));
						if (firstNotNull_CategoryName != null && categoryName2 != null && !firstNotNull_CategoryName.equals(categoryName2)) {
							categories_res.addLogEvent("Data consistency problem for [Categories - id :"+categories_res.getCategoryID()+"]: different values found for attribute 'Categories.categoryName': " + firstNotNull_CategoryName + " and " + categoryName2 + "." );
							logger.warn("Data consistency problem for [Categories - id :"+categories_res.getCategoryID()+"]: different values found for attribute 'Categories.categoryName': " + firstNotNull_CategoryName + " and " + categoryName2 + "." );
						}
						if (firstNotNull_CategoryName == null && categoryName2 != null) {
							firstNotNull_CategoryName = categoryName2;
						}
					}
					categories_res.setCategoryName(firstNotNull_CategoryName);
					
					// attribute 'Categories.description'
					String firstNotNull_Description = Util.getStringValue(r.getAs("description"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String description2 = Util.getStringValue(r.getAs("description_" + i));
						if (firstNotNull_Description != null && description2 != null && !firstNotNull_Description.equals(description2)) {
							categories_res.addLogEvent("Data consistency problem for [Categories - id :"+categories_res.getCategoryID()+"]: different values found for attribute 'Categories.description': " + firstNotNull_Description + " and " + description2 + "." );
							logger.warn("Data consistency problem for [Categories - id :"+categories_res.getCategoryID()+"]: different values found for attribute 'Categories.description': " + firstNotNull_Description + " and " + description2 + "." );
						}
						if (firstNotNull_Description == null && description2 != null) {
							firstNotNull_Description = description2;
						}
					}
					categories_res.setDescription(firstNotNull_Description);
					
					// attribute 'Categories.picture'
					byte[] firstNotNull_Picture = Util.getByteArrayValue(r.getAs("picture"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						byte[] picture2 = Util.getByteArrayValue(r.getAs("picture_" + i));
						if (firstNotNull_Picture != null && picture2 != null && !firstNotNull_Picture.equals(picture2)) {
							categories_res.addLogEvent("Data consistency problem for [Categories - id :"+categories_res.getCategoryID()+"]: different values found for attribute 'Categories.picture': " + firstNotNull_Picture + " and " + picture2 + "." );
							logger.warn("Data consistency problem for [Categories - id :"+categories_res.getCategoryID()+"]: different values found for attribute 'Categories.picture': " + firstNotNull_Picture + " and " + picture2 + "." );
						}
						if (firstNotNull_Picture == null && picture2 != null) {
							firstNotNull_Picture = picture2;
						}
					}
					categories_res.setPicture(firstNotNull_Picture);
	
					WrappedArray logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							categories_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							categories_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					return categories_res;
				}, Encoders.bean(Categories.class));
			return d;
	}
	
	
	
	
	
	
	public Categories getCategories(Categories.typeOf role, Products products) {
		if(role != null) {
			if(role.equals(Categories.typeOf.category))
				return getCategoryInTypeOfByProduct(products);
		}
		return null;
	}
	
	public Dataset<Categories> getCategoriesList(Categories.typeOf role, Condition<ProductsAttribute> condition) {
		if(role != null) {
			if(role.equals(Categories.typeOf.category))
				return getCategoryListInTypeOfByProductCondition(condition);
		}
		return null;
	}
	
	public Dataset<Categories> getCategoriesList(Categories.typeOf role, Condition<ProductsAttribute> condition1, Condition<CategoriesAttribute> condition2) {
		if(role != null) {
			if(role.equals(Categories.typeOf.category))
				return getCategoryListInTypeOf(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	
	
	
	public abstract Dataset<Categories> getCategoryListInTypeOf(conditions.Condition<conditions.ProductsAttribute> product_condition,conditions.Condition<conditions.CategoriesAttribute> category_condition);
	
	public Dataset<Categories> getCategoryListInTypeOfByProductCondition(conditions.Condition<conditions.ProductsAttribute> product_condition){
		return getCategoryListInTypeOf(product_condition, null);
	}
	
	public Categories getCategoryInTypeOfByProduct(pojo.Products product){
		if(product == null)
			return null;
	
		Condition c;
		c=Condition.simple(ProductsAttribute.productId,Operator.EQUALS, product.getProductId());
		Dataset<Categories> res = getCategoryListInTypeOfByProductCondition(c);
		return !res.isEmpty()?res.first():null;
	}
	
	public Dataset<Categories> getCategoryListInTypeOfByCategoryCondition(conditions.Condition<conditions.CategoriesAttribute> category_condition){
		return getCategoryListInTypeOf(null, category_condition);
	}
	
	
	public abstract boolean insertCategories(Categories categories);
	
	public abstract boolean insertCategoriesInCategoriesFromMyRelDB(Categories categories); 
	private boolean inUpdateMethod = false;
	private List<Row> allCategoriesIdList = null;
	public abstract void updateCategoriesList(conditions.Condition<conditions.CategoriesAttribute> condition, conditions.SetClause<conditions.CategoriesAttribute> set);
	
	public void updateCategories(pojo.Categories categories) {
		//TODO using the id
		return;
	}
	public abstract void updateCategoryListInTypeOf(
		conditions.Condition<conditions.ProductsAttribute> product_condition,
		conditions.Condition<conditions.CategoriesAttribute> category_condition,
		
		conditions.SetClause<conditions.CategoriesAttribute> set
	);
	
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
	
	
	public abstract void deleteCategoriesList(conditions.Condition<conditions.CategoriesAttribute> condition);
	
	public void deleteCategories(pojo.Categories categories) {
		//TODO using the id
		return;
	}
	public abstract void deleteCategoryListInTypeOf(	
		conditions.Condition<conditions.ProductsAttribute> product_condition,	
		conditions.Condition<conditions.CategoriesAttribute> category_condition);
	
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
