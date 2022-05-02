package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import pojo.Products;
import conditions.ProductsAttribute;
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
import conditions.ProductsAttribute;
import pojo.Supply;
import conditions.SuppliersAttribute;
import pojo.Suppliers;
import conditions.ProductsAttribute;
import pojo.TypeOf;
import conditions.CategoriesAttribute;
import pojo.Categories;
import conditions.ProductsAttribute;
import pojo.ComposedOf;
import conditions.OrdersAttribute;
import pojo.Orders;

public abstract class ProductsService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProductsService.class);
	protected SupplyService supplyService = new dao.impl.SupplyServiceImpl();
	protected TypeOfService typeOfService = new dao.impl.TypeOfServiceImpl();
	protected ComposedOfService composedOfService = new dao.impl.ComposedOfServiceImpl();
	


	public static enum ROLE_NAME {
		SUPPLY_SUPPLIEDPRODUCT, TYPEOF_PRODUCT, COMPOSEDOF_ORDEREDPRODUCTS
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.SUPPLY_SUPPLIEDPRODUCT, loading.Loading.EAGER);
		defaultLoadingParameters.put(ROLE_NAME.TYPEOF_PRODUCT, loading.Loading.EAGER);
		defaultLoadingParameters.put(ROLE_NAME.COMPOSEDOF_ORDEREDPRODUCTS, loading.Loading.LAZY);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public ProductsService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public ProductsService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
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
	
	
	public Dataset<Products> getProductsList(){
		return getProductsList(null);
	}
	
	public Dataset<Products> getProductsList(conditions.Condition<conditions.ProductsAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Products>> datasets = new ArrayList<Dataset<Products>>();
		Dataset<Products> d = null;
		d = getProductsListInProductsFromMyRelDB(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsProducts(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Products>) r -> condition == null || condition.evaluate(r));
		d = d.dropDuplicates(new String[] {"productId"});
		return d;
	}
	
	
	
	
	
	public abstract Dataset<Products> getProductsListInProductsFromMyRelDB(conditions.Condition<conditions.ProductsAttribute> condition, MutableBoolean refilterFlag);
	
	
	public Products getProductsById(Integer productId){
		Condition cond;
		cond = Condition.simple(ProductsAttribute.productId, conditions.Operator.EQUALS, productId);
		Dataset<Products> res = getProductsList(cond);
		if(res!=null && !res.isEmpty())
			return res.first();
		return null;
	}
	
	public Dataset<Products> getProductsListByProductId(Integer productId) {
		return getProductsList(conditions.Condition.simple(conditions.ProductsAttribute.productId, conditions.Operator.EQUALS, productId));
	}
	
	public Dataset<Products> getProductsListByProductName(String ProductName) {
		return getProductsList(conditions.Condition.simple(conditions.ProductsAttribute.ProductName, conditions.Operator.EQUALS, ProductName));
	}
	
	public Dataset<Products> getProductsListByQuantityPerUnit(String QuantityPerUnit) {
		return getProductsList(conditions.Condition.simple(conditions.ProductsAttribute.QuantityPerUnit, conditions.Operator.EQUALS, QuantityPerUnit));
	}
	
	public Dataset<Products> getProductsListByUnitPrice(Double UnitPrice) {
		return getProductsList(conditions.Condition.simple(conditions.ProductsAttribute.UnitPrice, conditions.Operator.EQUALS, UnitPrice));
	}
	
	public Dataset<Products> getProductsListByUnitsInStock(Integer UnitsInStock) {
		return getProductsList(conditions.Condition.simple(conditions.ProductsAttribute.UnitsInStock, conditions.Operator.EQUALS, UnitsInStock));
	}
	
	public Dataset<Products> getProductsListByUnitsOnOrder(Integer UnitsOnOrder) {
		return getProductsList(conditions.Condition.simple(conditions.ProductsAttribute.UnitsOnOrder, conditions.Operator.EQUALS, UnitsOnOrder));
	}
	
	public Dataset<Products> getProductsListByReorderLevel(Integer ReorderLevel) {
		return getProductsList(conditions.Condition.simple(conditions.ProductsAttribute.ReorderLevel, conditions.Operator.EQUALS, ReorderLevel));
	}
	
	public Dataset<Products> getProductsListByDiscontinued(Boolean Discontinued) {
		return getProductsList(conditions.Condition.simple(conditions.ProductsAttribute.Discontinued, conditions.Operator.EQUALS, Discontinued));
	}
	
	
	
	public static Dataset<Products> fullOuterJoinsProducts(List<Dataset<Products>> datasetsPOJO) {
		return fullOuterJoinsProducts(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Products> fullLeftOuterJoinsProducts(List<Dataset<Products>> datasetsPOJO) {
		return fullOuterJoinsProducts(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Products> fullOuterJoinsProducts(List<Dataset<Products>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Products> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("productId");
			logger.debug("Start {} of [{}] datasets of [Products] objects",joinMode,datasetsPOJO.size());
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("productName", "productName_1")
								.withColumnRenamed("quantityPerUnit", "quantityPerUnit_1")
								.withColumnRenamed("unitPrice", "unitPrice_1")
								.withColumnRenamed("unitsInStock", "unitsInStock_1")
								.withColumnRenamed("unitsOnOrder", "unitsOnOrder_1")
								.withColumnRenamed("reorderLevel", "reorderLevel_1")
								.withColumnRenamed("discontinued", "discontinued_1")
								.withColumnRenamed("logEvents", "logEvents_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("productName", "productName_" + i)
								.withColumnRenamed("quantityPerUnit", "quantityPerUnit_" + i)
								.withColumnRenamed("unitPrice", "unitPrice_" + i)
								.withColumnRenamed("unitsInStock", "unitsInStock_" + i)
								.withColumnRenamed("unitsOnOrder", "unitsOnOrder_" + i)
								.withColumnRenamed("reorderLevel", "reorderLevel_" + i)
								.withColumnRenamed("discontinued", "discontinued_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
						, seq, joinMode);
			}
			logger.debug("End join. Start");
			logger.debug("Start transforming Row objects to [Products] objects"); 
			d = res.map((MapFunction<Row, Products>) r -> {
					Products products_res = new Products();
					
					// attribute 'Products.productId'
					Integer firstNotNull_productId = Util.getIntegerValue(r.getAs("productId"));
					products_res.setProductId(firstNotNull_productId);
					
					// attribute 'Products.productName'
					String firstNotNull_ProductName = Util.getStringValue(r.getAs("productName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String productName2 = Util.getStringValue(r.getAs("productName_" + i));
						if (firstNotNull_ProductName != null && productName2 != null && !firstNotNull_ProductName.equals(productName2)) {
							products_res.addLogEvent("Data consistency problem for [Products - id :"+products_res.getProductId()+"]: different values found for attribute 'Products.productName': " + firstNotNull_ProductName + " and " + productName2 + "." );
							logger.warn("Data consistency problem for [Products - id :"+products_res.getProductId()+"]: different values found for attribute 'Products.productName': " + firstNotNull_ProductName + " and " + productName2 + "." );
						}
						if (firstNotNull_ProductName == null && productName2 != null) {
							firstNotNull_ProductName = productName2;
						}
					}
					products_res.setProductName(firstNotNull_ProductName);
					
					// attribute 'Products.quantityPerUnit'
					String firstNotNull_QuantityPerUnit = Util.getStringValue(r.getAs("quantityPerUnit"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String quantityPerUnit2 = Util.getStringValue(r.getAs("quantityPerUnit_" + i));
						if (firstNotNull_QuantityPerUnit != null && quantityPerUnit2 != null && !firstNotNull_QuantityPerUnit.equals(quantityPerUnit2)) {
							products_res.addLogEvent("Data consistency problem for [Products - id :"+products_res.getProductId()+"]: different values found for attribute 'Products.quantityPerUnit': " + firstNotNull_QuantityPerUnit + " and " + quantityPerUnit2 + "." );
							logger.warn("Data consistency problem for [Products - id :"+products_res.getProductId()+"]: different values found for attribute 'Products.quantityPerUnit': " + firstNotNull_QuantityPerUnit + " and " + quantityPerUnit2 + "." );
						}
						if (firstNotNull_QuantityPerUnit == null && quantityPerUnit2 != null) {
							firstNotNull_QuantityPerUnit = quantityPerUnit2;
						}
					}
					products_res.setQuantityPerUnit(firstNotNull_QuantityPerUnit);
					
					// attribute 'Products.unitPrice'
					Double firstNotNull_UnitPrice = Util.getDoubleValue(r.getAs("unitPrice"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double unitPrice2 = Util.getDoubleValue(r.getAs("unitPrice_" + i));
						if (firstNotNull_UnitPrice != null && unitPrice2 != null && !firstNotNull_UnitPrice.equals(unitPrice2)) {
							products_res.addLogEvent("Data consistency problem for [Products - id :"+products_res.getProductId()+"]: different values found for attribute 'Products.unitPrice': " + firstNotNull_UnitPrice + " and " + unitPrice2 + "." );
							logger.warn("Data consistency problem for [Products - id :"+products_res.getProductId()+"]: different values found for attribute 'Products.unitPrice': " + firstNotNull_UnitPrice + " and " + unitPrice2 + "." );
						}
						if (firstNotNull_UnitPrice == null && unitPrice2 != null) {
							firstNotNull_UnitPrice = unitPrice2;
						}
					}
					products_res.setUnitPrice(firstNotNull_UnitPrice);
					
					// attribute 'Products.unitsInStock'
					Integer firstNotNull_UnitsInStock = Util.getIntegerValue(r.getAs("unitsInStock"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer unitsInStock2 = Util.getIntegerValue(r.getAs("unitsInStock_" + i));
						if (firstNotNull_UnitsInStock != null && unitsInStock2 != null && !firstNotNull_UnitsInStock.equals(unitsInStock2)) {
							products_res.addLogEvent("Data consistency problem for [Products - id :"+products_res.getProductId()+"]: different values found for attribute 'Products.unitsInStock': " + firstNotNull_UnitsInStock + " and " + unitsInStock2 + "." );
							logger.warn("Data consistency problem for [Products - id :"+products_res.getProductId()+"]: different values found for attribute 'Products.unitsInStock': " + firstNotNull_UnitsInStock + " and " + unitsInStock2 + "." );
						}
						if (firstNotNull_UnitsInStock == null && unitsInStock2 != null) {
							firstNotNull_UnitsInStock = unitsInStock2;
						}
					}
					products_res.setUnitsInStock(firstNotNull_UnitsInStock);
					
					// attribute 'Products.unitsOnOrder'
					Integer firstNotNull_UnitsOnOrder = Util.getIntegerValue(r.getAs("unitsOnOrder"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer unitsOnOrder2 = Util.getIntegerValue(r.getAs("unitsOnOrder_" + i));
						if (firstNotNull_UnitsOnOrder != null && unitsOnOrder2 != null && !firstNotNull_UnitsOnOrder.equals(unitsOnOrder2)) {
							products_res.addLogEvent("Data consistency problem for [Products - id :"+products_res.getProductId()+"]: different values found for attribute 'Products.unitsOnOrder': " + firstNotNull_UnitsOnOrder + " and " + unitsOnOrder2 + "." );
							logger.warn("Data consistency problem for [Products - id :"+products_res.getProductId()+"]: different values found for attribute 'Products.unitsOnOrder': " + firstNotNull_UnitsOnOrder + " and " + unitsOnOrder2 + "." );
						}
						if (firstNotNull_UnitsOnOrder == null && unitsOnOrder2 != null) {
							firstNotNull_UnitsOnOrder = unitsOnOrder2;
						}
					}
					products_res.setUnitsOnOrder(firstNotNull_UnitsOnOrder);
					
					// attribute 'Products.reorderLevel'
					Integer firstNotNull_ReorderLevel = Util.getIntegerValue(r.getAs("reorderLevel"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer reorderLevel2 = Util.getIntegerValue(r.getAs("reorderLevel_" + i));
						if (firstNotNull_ReorderLevel != null && reorderLevel2 != null && !firstNotNull_ReorderLevel.equals(reorderLevel2)) {
							products_res.addLogEvent("Data consistency problem for [Products - id :"+products_res.getProductId()+"]: different values found for attribute 'Products.reorderLevel': " + firstNotNull_ReorderLevel + " and " + reorderLevel2 + "." );
							logger.warn("Data consistency problem for [Products - id :"+products_res.getProductId()+"]: different values found for attribute 'Products.reorderLevel': " + firstNotNull_ReorderLevel + " and " + reorderLevel2 + "." );
						}
						if (firstNotNull_ReorderLevel == null && reorderLevel2 != null) {
							firstNotNull_ReorderLevel = reorderLevel2;
						}
					}
					products_res.setReorderLevel(firstNotNull_ReorderLevel);
					
					// attribute 'Products.discontinued'
					Boolean firstNotNull_Discontinued = Util.getBooleanValue(r.getAs("discontinued"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Boolean discontinued2 = Util.getBooleanValue(r.getAs("discontinued_" + i));
						if (firstNotNull_Discontinued != null && discontinued2 != null && !firstNotNull_Discontinued.equals(discontinued2)) {
							products_res.addLogEvent("Data consistency problem for [Products - id :"+products_res.getProductId()+"]: different values found for attribute 'Products.discontinued': " + firstNotNull_Discontinued + " and " + discontinued2 + "." );
							logger.warn("Data consistency problem for [Products - id :"+products_res.getProductId()+"]: different values found for attribute 'Products.discontinued': " + firstNotNull_Discontinued + " and " + discontinued2 + "." );
						}
						if (firstNotNull_Discontinued == null && discontinued2 != null) {
							firstNotNull_Discontinued = discontinued2;
						}
					}
					products_res.setDiscontinued(firstNotNull_Discontinued);
	
					WrappedArray logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							products_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							products_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					return products_res;
				}, Encoders.bean(Products.class));
			return d;
	}
	
	
	
	
	
	public Dataset<Products> getProductsList(Products.supply role, Suppliers suppliers) {
		if(role != null) {
			if(role.equals(Products.supply.suppliedProduct))
				return getSuppliedProductListInSupplyBySupplier(suppliers);
		}
		return null;
	}
	
	public Dataset<Products> getProductsList(Products.supply role, Condition<SuppliersAttribute> condition) {
		if(role != null) {
			if(role.equals(Products.supply.suppliedProduct))
				return getSuppliedProductListInSupplyBySupplierCondition(condition);
		}
		return null;
	}
	
	public Dataset<Products> getProductsList(Products.supply role, Condition<ProductsAttribute> condition1, Condition<SuppliersAttribute> condition2) {
		if(role != null) {
			if(role.equals(Products.supply.suppliedProduct))
				return getSuppliedProductListInSupply(condition1, condition2);
		}
		return null;
	}
	
	
	
	public Dataset<Products> getProductsList(Products.typeOf role, Categories categories) {
		if(role != null) {
			if(role.equals(Products.typeOf.product))
				return getProductListInTypeOfByCategory(categories);
		}
		return null;
	}
	
	public Dataset<Products> getProductsList(Products.typeOf role, Condition<CategoriesAttribute> condition) {
		if(role != null) {
			if(role.equals(Products.typeOf.product))
				return getProductListInTypeOfByCategoryCondition(condition);
		}
		return null;
	}
	
	public Dataset<Products> getProductsList(Products.typeOf role, Condition<ProductsAttribute> condition1, Condition<CategoriesAttribute> condition2) {
		if(role != null) {
			if(role.equals(Products.typeOf.product))
				return getProductListInTypeOf(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	
	
	
	public abstract Dataset<Products> getSuppliedProductListInSupply(conditions.Condition<conditions.ProductsAttribute> suppliedProduct_condition,conditions.Condition<conditions.SuppliersAttribute> supplier_condition);
	
	public Dataset<Products> getSuppliedProductListInSupplyBySuppliedProductCondition(conditions.Condition<conditions.ProductsAttribute> suppliedProduct_condition){
		return getSuppliedProductListInSupply(suppliedProduct_condition, null);
	}
	public Dataset<Products> getSuppliedProductListInSupplyBySupplierCondition(conditions.Condition<conditions.SuppliersAttribute> supplier_condition){
		return getSuppliedProductListInSupply(null, supplier_condition);
	}
	
	public Dataset<Products> getSuppliedProductListInSupplyBySupplier(pojo.Suppliers supplier){
		if(supplier == null)
			return null;
	
		Condition c;
		c=Condition.simple(SuppliersAttribute.supplierId,Operator.EQUALS, supplier.getSupplierId());
		Dataset<Products> res = getSuppliedProductListInSupplyBySupplierCondition(c);
		return res;
	}
	
	public abstract Dataset<Products> getProductListInTypeOf(conditions.Condition<conditions.ProductsAttribute> product_condition,conditions.Condition<conditions.CategoriesAttribute> category_condition);
	
	public Dataset<Products> getProductListInTypeOfByProductCondition(conditions.Condition<conditions.ProductsAttribute> product_condition){
		return getProductListInTypeOf(product_condition, null);
	}
	public Dataset<Products> getProductListInTypeOfByCategoryCondition(conditions.Condition<conditions.CategoriesAttribute> category_condition){
		return getProductListInTypeOf(null, category_condition);
	}
	
	public Dataset<Products> getProductListInTypeOfByCategory(pojo.Categories category){
		if(category == null)
			return null;
	
		Condition c;
		c=Condition.simple(CategoriesAttribute.categoryID,Operator.EQUALS, category.getCategoryID());
		Dataset<Products> res = getProductListInTypeOfByCategoryCondition(c);
		return res;
	}
	
	public abstract Dataset<Products> getOrderedProductsListInComposedOf(conditions.Condition<conditions.OrdersAttribute> order_condition,conditions.Condition<conditions.ProductsAttribute> orderedProducts_condition, conditions.Condition<conditions.ComposedOfAttribute> composedOf_condition);
	
	public Dataset<Products> getOrderedProductsListInComposedOfByOrderCondition(conditions.Condition<conditions.OrdersAttribute> order_condition){
		return getOrderedProductsListInComposedOf(order_condition, null, null);
	}
	
	public Dataset<Products> getOrderedProductsListInComposedOfByOrder(pojo.Orders order){
		if(order == null)
			return null;
	
		Condition c;
		c=Condition.simple(OrdersAttribute.id,Operator.EQUALS, order.getId());
		Dataset<Products> res = getOrderedProductsListInComposedOfByOrderCondition(c);
		return res;
	}
	
	public Dataset<Products> getOrderedProductsListInComposedOfByOrderedProductsCondition(conditions.Condition<conditions.ProductsAttribute> orderedProducts_condition){
		return getOrderedProductsListInComposedOf(null, orderedProducts_condition, null);
	}
	public Dataset<Products> getOrderedProductsListInComposedOfByComposedOfCondition(
		conditions.Condition<conditions.ComposedOfAttribute> composedOf_condition
	){
		return getOrderedProductsListInComposedOf(null, null, composedOf_condition);
	}
	
	
	public abstract boolean insertProducts(Products products);
	
	public abstract boolean insertProductsInProductsFromMyRelDB(Products products); 
	private boolean inUpdateMethod = false;
	private List<Row> allProductsIdList = null;
	public abstract void updateProductsList(conditions.Condition<conditions.ProductsAttribute> condition, conditions.SetClause<conditions.ProductsAttribute> set);
	
	public void updateProducts(pojo.Products products) {
		//TODO using the id
		return;
	}
	public abstract void updateSuppliedProductListInSupply(
		conditions.Condition<conditions.ProductsAttribute> suppliedProduct_condition,
		conditions.Condition<conditions.SuppliersAttribute> supplier_condition,
		
		conditions.SetClause<conditions.ProductsAttribute> set
	);
	
	public void updateSuppliedProductListInSupplyBySuppliedProductCondition(
		conditions.Condition<conditions.ProductsAttribute> suppliedProduct_condition,
		conditions.SetClause<conditions.ProductsAttribute> set
	){
		updateSuppliedProductListInSupply(suppliedProduct_condition, null, set);
	}
	public void updateSuppliedProductListInSupplyBySupplierCondition(
		conditions.Condition<conditions.SuppliersAttribute> supplier_condition,
		conditions.SetClause<conditions.ProductsAttribute> set
	){
		updateSuppliedProductListInSupply(null, supplier_condition, set);
	}
	
	public void updateSuppliedProductListInSupplyBySupplier(
		pojo.Suppliers supplier,
		conditions.SetClause<conditions.ProductsAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public abstract void updateProductListInTypeOf(
		conditions.Condition<conditions.ProductsAttribute> product_condition,
		conditions.Condition<conditions.CategoriesAttribute> category_condition,
		
		conditions.SetClause<conditions.ProductsAttribute> set
	);
	
	public void updateProductListInTypeOfByProductCondition(
		conditions.Condition<conditions.ProductsAttribute> product_condition,
		conditions.SetClause<conditions.ProductsAttribute> set
	){
		updateProductListInTypeOf(product_condition, null, set);
	}
	public void updateProductListInTypeOfByCategoryCondition(
		conditions.Condition<conditions.CategoriesAttribute> category_condition,
		conditions.SetClause<conditions.ProductsAttribute> set
	){
		updateProductListInTypeOf(null, category_condition, set);
	}
	
	public void updateProductListInTypeOfByCategory(
		pojo.Categories category,
		conditions.SetClause<conditions.ProductsAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public abstract void updateOrderedProductsListInComposedOf(
		conditions.Condition<conditions.OrdersAttribute> order_condition,
		conditions.Condition<conditions.ProductsAttribute> orderedProducts_condition,
		conditions.Condition<conditions.ComposedOfAttribute> composedOf,
		conditions.SetClause<conditions.ProductsAttribute> set
	);
	
	public void updateOrderedProductsListInComposedOfByOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> order_condition,
		conditions.SetClause<conditions.ProductsAttribute> set
	){
		updateOrderedProductsListInComposedOf(order_condition, null, null, set);
	}
	
	public void updateOrderedProductsListInComposedOfByOrder(
		pojo.Orders order,
		conditions.SetClause<conditions.ProductsAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateOrderedProductsListInComposedOfByOrderedProductsCondition(
		conditions.Condition<conditions.ProductsAttribute> orderedProducts_condition,
		conditions.SetClause<conditions.ProductsAttribute> set
	){
		updateOrderedProductsListInComposedOf(null, orderedProducts_condition, null, set);
	}
	public void updateOrderedProductsListInComposedOfByComposedOfCondition(
		conditions.Condition<conditions.ComposedOfAttribute> composedOf_condition,
		conditions.SetClause<conditions.ProductsAttribute> set
	){
		updateOrderedProductsListInComposedOf(null, null, composedOf_condition, set);
	}
	
	
	public abstract void deleteProductsList(conditions.Condition<conditions.ProductsAttribute> condition);
	
	public void deleteProducts(pojo.Products products) {
		//TODO using the id
		return;
	}
	public abstract void deleteSuppliedProductListInSupply(	
		conditions.Condition<conditions.ProductsAttribute> suppliedProduct_condition,	
		conditions.Condition<conditions.SuppliersAttribute> supplier_condition);
	
	public void deleteSuppliedProductListInSupplyBySuppliedProductCondition(
		conditions.Condition<conditions.ProductsAttribute> suppliedProduct_condition
	){
		deleteSuppliedProductListInSupply(suppliedProduct_condition, null);
	}
	public void deleteSuppliedProductListInSupplyBySupplierCondition(
		conditions.Condition<conditions.SuppliersAttribute> supplier_condition
	){
		deleteSuppliedProductListInSupply(null, supplier_condition);
	}
	
	public void deleteSuppliedProductListInSupplyBySupplier(
		pojo.Suppliers supplier 
	){
		//TODO get id in condition
		return;	
	}
	
	public abstract void deleteProductListInTypeOf(	
		conditions.Condition<conditions.ProductsAttribute> product_condition,	
		conditions.Condition<conditions.CategoriesAttribute> category_condition);
	
	public void deleteProductListInTypeOfByProductCondition(
		conditions.Condition<conditions.ProductsAttribute> product_condition
	){
		deleteProductListInTypeOf(product_condition, null);
	}
	public void deleteProductListInTypeOfByCategoryCondition(
		conditions.Condition<conditions.CategoriesAttribute> category_condition
	){
		deleteProductListInTypeOf(null, category_condition);
	}
	
	public void deleteProductListInTypeOfByCategory(
		pojo.Categories category 
	){
		//TODO get id in condition
		return;	
	}
	
	public abstract void deleteOrderedProductsListInComposedOf(	
		conditions.Condition<conditions.OrdersAttribute> order_condition,	
		conditions.Condition<conditions.ProductsAttribute> orderedProducts_condition,
		conditions.Condition<conditions.ComposedOfAttribute> composedOf);
	
	public void deleteOrderedProductsListInComposedOfByOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> order_condition
	){
		deleteOrderedProductsListInComposedOf(order_condition, null, null);
	}
	
	public void deleteOrderedProductsListInComposedOfByOrder(
		pojo.Orders order 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteOrderedProductsListInComposedOfByOrderedProductsCondition(
		conditions.Condition<conditions.ProductsAttribute> orderedProducts_condition
	){
		deleteOrderedProductsListInComposedOf(null, orderedProducts_condition, null);
	}
	public void deleteOrderedProductsListInComposedOfByComposedOfCondition(
		conditions.Condition<conditions.ComposedOfAttribute> composedOf_condition
	){
		deleteOrderedProductsListInComposedOf(null, null, composedOf_condition);
	}
	
}
