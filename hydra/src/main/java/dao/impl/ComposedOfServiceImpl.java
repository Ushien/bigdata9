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
import conditions.ComposedOfAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.OrdersTDO;
import tdo.ComposedOfTDO;
import conditions.OrdersAttribute;
import dao.services.OrdersService;
import tdo.ProductsTDO;
import tdo.ComposedOfTDO;
import conditions.ProductsAttribute;
import dao.services.ProductsService;
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

public class ComposedOfServiceImpl extends dao.services.ComposedOfService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ComposedOfServiceImpl.class);
	// A<-AB->B . getAListInREL
	//join structure
	// Left side 'OrderRef' of reference [order ]
	public Dataset<OrdersTDO> getOrdersTDOListOrderInOrderInOrdersFromMongoDB(Condition<OrdersAttribute> condition, MutableBoolean refilterFlag){	
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
					
						// field  OrderID for reference order . Reference field : OrderID
					nestedRow =  r1;
					if(nestedRow != null) {
						orders1.setRelDB_Order_Details_order_OrderID(nestedRow.getAs("OrderID") == null ? null : nestedRow.getAs("OrderID").toString());
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
	
	public static Pair<String, List<String>> getSQLWhereClauseInOrder_DetailsFromMyRelDB(Condition<ComposedOfAttribute> condition, MutableBoolean refilterFlag) {
		return getSQLWhereClauseInOrder_DetailsFromMyRelDBWithTableAlias(condition, refilterFlag, "");
	}
	
	public static List<String> getSQLSetClauseInOrder_DetailsFromMyRelDB(conditions.SetClause<ComposedOfAttribute> set) {
		List<String> res = new ArrayList<String>();
		if(set != null) {
			java.util.Map<String, java.util.Map<String, String>> longFieldValues = new java.util.HashMap<String, java.util.Map<String, String>>();
			java.util.Map<ComposedOfAttribute, Object> clause = set.getClause();
			for(java.util.Map.Entry<ComposedOfAttribute, Object> e : clause.entrySet()) {
				ComposedOfAttribute attr = e.getKey();
				Object value = e.getValue();
				if(attr == ComposedOfAttribute.unitPrice ) {
					res.add("UnitPrice = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ComposedOfAttribute.quantity ) {
					res.add("Quantity = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ComposedOfAttribute.discount ) {
					res.add("Discount = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
			}
	
			for(java.util.Map.Entry<String, java.util.Map<String, String>> entry : longFieldValues.entrySet()) {
				String longField = entry.getKey();
				java.util.Map<String, String> values = entry.getValue();
			}
	
		}
		return res;
	}
	
	public static Pair<String, List<String>> getSQLWhereClauseInOrder_DetailsFromMyRelDBWithTableAlias(Condition<ComposedOfAttribute> condition, MutableBoolean refilterFlag, String tableAlias) {
		String where = null;	
		List<String> preparedValues = new java.util.ArrayList<String>();
		if(condition != null) {
			
			if(condition instanceof SimpleCondition) {
				ComposedOfAttribute attr = ((SimpleCondition<ComposedOfAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<ComposedOfAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<ComposedOfAttribute>) condition).getValue();
				if(value != null) {
					boolean isConditionAttrEncountered = false;
					if(attr == ComposedOfAttribute.unitPrice ) {
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
						
						where = tableAlias + "UnitPrice " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == ComposedOfAttribute.quantity ) {
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
						
						where = tableAlias + "Quantity " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == ComposedOfAttribute.discount ) {
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
						
						where = tableAlias + "Discount " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						where = "1 = 1";
					}
				} else {
					if(attr == ComposedOfAttribute.unitPrice ) {
						if(op == Operator.EQUALS)
							where =  "UnitPrice IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "UnitPrice IS NOT NULL";
					}
					if(attr == ComposedOfAttribute.quantity ) {
						if(op == Operator.EQUALS)
							where =  "Quantity IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "Quantity IS NOT NULL";
					}
					if(attr == ComposedOfAttribute.discount ) {
						if(op == Operator.EQUALS)
							where =  "Discount IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "Discount IS NOT NULL";
					}
				}
			}
	
			if(condition instanceof AndCondition) {
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInOrder_DetailsFromMyRelDB(((AndCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInOrder_DetailsFromMyRelDB(((AndCondition) condition).getRightCondition(), refilterFlag);
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
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInOrder_DetailsFromMyRelDB(((OrCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInOrder_DetailsFromMyRelDB(((OrCondition) condition).getRightCondition(), refilterFlag);
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
	
	
	
	
	
	// A<-AB->B . getBListInREL
	
	
	
	public Dataset<ComposedOfTDO> getComposedOfTDOListInProductsAndOrder_DetailsFrommyRelDB(Condition<ProductsAttribute> orderedProducts_cond, Condition<ComposedOfAttribute> composedOf_cond, MutableBoolean refilterFlag, MutableBoolean composedOf_refilter) {
		Pair<String, List<String>> whereClause = ProductsServiceImpl.getSQLWhereClauseInProductsFromMyRelDBWithTableAlias(orderedProducts_cond, refilterFlag, "Products.");
		Pair<String, List<String>> whereClause2 = ComposedOfServiceImpl.getSQLWhereClauseInOrder_DetailsFromMyRelDBWithTableAlias(composedOf_cond, composedOf_refilter, "Order_Details.");
		
		String where1 = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where1 = where1.replaceFirst("\\?", preparedValue);
		}
	
		String where2 = whereClause2.getKey();
		preparedValues = whereClause2.getValue();
		for(String preparedValue : preparedValues) {
			where2 = where2.replaceFirst("\\?", preparedValue);
		}
		
		String where = "";
		if(where1 != null)
			where = " AND " + where1;
		if(where2 != null)
			where = " AND " + where2;
	
		String aliasedColumns = "Products.ProductID as Products_ProductID,Products.ProductName as Products_ProductName,Products.QuantityPerUnit as Products_QuantityPerUnit,Products.UnitPrice as Products_UnitPrice,Products.UnitsInStock as Products_UnitsInStock,Products.UnitsOnOrder as Products_UnitsOnOrder,Products.ReorderLevel as Products_ReorderLevel,Products.Discontinued as Products_Discontinued,Products.SupplierRef as Products_SupplierRef,Products.CategoryRef as Products_CategoryRef, Order_Details.OrderRef as Order_Details_OrderRef,Order_Details.ProductRef as Order_Details_ProductRef,Order_Details.UnitPrice as Order_Details_UnitPrice,Order_Details.Quantity as Order_Details_Quantity,Order_Details.Discount as Order_Details_Discount";
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("myRelDB", "(SELECT " + aliasedColumns + " FROM Products, Order_Details WHERE Order_Details.ProductRef = Products.ProductID" + where + ") AS JOIN_TABLE", null);
		Dataset<ComposedOfTDO> res = d.map((MapFunction<Row, ComposedOfTDO>) r -> {
					ComposedOfTDO composedOf_res = new ComposedOfTDO();
					composedOf_res.setOrderedProducts(new Products());
					
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
					// attribute [ComposedOf.UnitPrice]
					Double composedOf_unitPrice = Util.getDoubleValue(r.getAs("Order_Details_UnitPrice"));
					composedOf_res.setUnitPrice(composedOf_unitPrice);
					
					// attribute [ComposedOf.Quantity]
					Integer composedOf_quantity = Util.getIntegerValue(r.getAs("Order_Details_Quantity"));
					composedOf_res.setQuantity(composedOf_quantity);
					
					// attribute [ComposedOf.Discount]
					Double composedOf_discount = Util.getDoubleValue(r.getAs("Order_Details_Discount"));
					composedOf_res.setDiscount(composedOf_discount);
		
		
					
					// attribute [Products.ProductId]
					Integer products_productId = Util.getIntegerValue(r.getAs("Products_ProductID"));
					composedOf_res.getOrderedProducts().setProductId(products_productId);
					
					// attribute [Products.ProductName]
					String products_productName = Util.getStringValue(r.getAs("Products_ProductName"));
					composedOf_res.getOrderedProducts().setProductName(products_productName);
					
					// attribute [Products.QuantityPerUnit]
					String products_quantityPerUnit = Util.getStringValue(r.getAs("Products_QuantityPerUnit"));
					composedOf_res.getOrderedProducts().setQuantityPerUnit(products_quantityPerUnit);
					
					// attribute [Products.UnitPrice]
					Double products_unitPrice = Util.getDoubleValue(r.getAs("Products_UnitPrice"));
					composedOf_res.getOrderedProducts().setUnitPrice(products_unitPrice);
					
					// attribute [Products.UnitsInStock]
					Integer products_unitsInStock = Util.getIntegerValue(r.getAs("Products_UnitsInStock"));
					composedOf_res.getOrderedProducts().setUnitsInStock(products_unitsInStock);
					
					// attribute [Products.UnitsOnOrder]
					Integer products_unitsOnOrder = Util.getIntegerValue(r.getAs("Products_UnitsOnOrder"));
					composedOf_res.getOrderedProducts().setUnitsOnOrder(products_unitsOnOrder);
					
					// attribute [Products.ReorderLevel]
					Integer products_reorderLevel = Util.getIntegerValue(r.getAs("Products_ReorderLevel"));
					composedOf_res.getOrderedProducts().setReorderLevel(products_reorderLevel);
					
					// attribute [Products.Discontinued]
					Boolean products_discontinued = Util.getBooleanValue(r.getAs("Products_Discontinued"));
					composedOf_res.getOrderedProducts().setDiscontinued(products_discontinued);
					
					String order_OrderRef = r.getAs("Order_Details_OrderRef") == null ? null : r.getAs("Order_Details_OrderRef").toString();
					composedOf_res.setRelDB_Order_Details_order_OrderRef(order_OrderRef);
					
		
					return composedOf_res;
				}, Encoders.bean(ComposedOfTDO.class));
		
		return res;
	}
	
	
	
	
	
	
	public Dataset<ComposedOf> getComposedOfList(
		Condition<OrdersAttribute> order_condition,
		Condition<ProductsAttribute> orderedProducts_condition,
		Condition<ComposedOfAttribute> composedOf_condition
	){
		ComposedOfServiceImpl composedOfService = this;
		OrdersService ordersService = new OrdersServiceImpl();  
		ProductsService productsService = new ProductsServiceImpl();
		MutableBoolean order_refilter = new MutableBoolean(false);
		List<Dataset<ComposedOf>> datasetsPOJO = new ArrayList<Dataset<ComposedOf>>();
		boolean all_already_persisted = false;
		MutableBoolean orderedProducts_refilter = new MutableBoolean(false);
		MutableBoolean composedOf_refilter = new MutableBoolean(false);
		org.apache.spark.sql.Column joinCondition = null;
		// join physical structure A<-AB->B
		//join between 2 SQL tables and a non-relational structure
		// (A) (AB - B)
		orderedProducts_refilter = new MutableBoolean(false);
		Dataset<ComposedOfTDO> res_composedOf_purchasedProducts_order = composedOfService.getComposedOfTDOListInProductsAndOrder_DetailsFrommyRelDB(orderedProducts_condition, composedOf_condition, orderedProducts_refilter, composedOf_refilter);
		Dataset<OrdersTDO> res_order_purchasedProducts = composedOfService.getOrdersTDOListOrderInOrderInOrdersFromMongoDB(order_condition, order_refilter);
		Dataset<Row> B_res_order_purchasedProducts = res_order_purchasedProducts
			.withColumnRenamed("id", "B_id")
			.withColumnRenamed("orderDate", "B_orderDate")
			.withColumnRenamed("requiredDate", "B_requiredDate")
			.withColumnRenamed("shippedDate", "B_shippedDate")
			.withColumnRenamed("freight", "B_freight")
			.withColumnRenamed("shipName", "B_shipName")
			.withColumnRenamed("shipAddress", "B_shipAddress")
			.withColumnRenamed("shipCity", "B_shipCity")
			.withColumnRenamed("shipRegion", "B_shipRegion")
			.withColumnRenamed("shipPostalCode", "B_shipPostalCode")
			.withColumnRenamed("shipCountry", "B_shipCountry")
			.withColumnRenamed("logEvents", "B_logEvents");
		
		Dataset<Row> res_row_purchasedProducts_order = res_composedOf_purchasedProducts_order.join(B_res_order_purchasedProducts,
			res_composedOf_purchasedProducts_order.col("relDB_Order_Details_order_OrderRef").equalTo(B_res_order_purchasedProducts.col("relDB_Order_Details_order_OrderID")));
		Dataset<ComposedOf> res_Orders_order = res_row_purchasedProducts_order.map((MapFunction<Row, ComposedOf>) r -> {
					ComposedOf res = new ComposedOf();
					
					Orders B = new Orders();
					Products A = new Products();
						
					Object o = r.getAs("orderedProducts");
					if(o != null) {
						if(o instanceof Row) {
							Row r2 = (Row) o;
							A.setProductId(Util.getIntegerValue(r2.getAs("productId")));
							A.setProductName(Util.getStringValue(r2.getAs("productName")));
							A.setQuantityPerUnit(Util.getStringValue(r2.getAs("quantityPerUnit")));
							A.setUnitPrice(Util.getDoubleValue(r2.getAs("unitPrice")));
							A.setUnitsInStock(Util.getIntegerValue(r2.getAs("unitsInStock")));
							A.setUnitsOnOrder(Util.getIntegerValue(r2.getAs("unitsOnOrder")));
							A.setReorderLevel(Util.getIntegerValue(r2.getAs("reorderLevel")));
							A.setDiscontinued(Util.getBooleanValue(r2.getAs("discontinued")));
							A.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r2.getAs("logEvents")));
						}
						if(o instanceof Products)
							A = (Products) o;
					}
		
		
					res.setUnitPrice(Util.getDoubleValue(r.getAs("unitPrice")));
					res.setQuantity(Util.getIntegerValue(r.getAs("quantity")));
					res.setDiscount(Util.getDoubleValue(r.getAs("discount")));
					res.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("logEvents")));
		
					B.setId(Util.getIntegerValue(r.getAs("B_id")));
					B.setOrderDate(Util.getLocalDateValue(r.getAs("B_orderDate")));
					B.setRequiredDate(Util.getLocalDateValue(r.getAs("B_requiredDate")));
					B.setShippedDate(Util.getLocalDateValue(r.getAs("B_shippedDate")));
					B.setFreight(Util.getDoubleValue(r.getAs("B_freight")));
					B.setShipName(Util.getStringValue(r.getAs("B_shipName")));
					B.setShipAddress(Util.getStringValue(r.getAs("B_shipAddress")));
					B.setShipCity(Util.getStringValue(r.getAs("B_shipCity")));
					B.setShipRegion(Util.getStringValue(r.getAs("B_shipRegion")));
					B.setShipPostalCode(Util.getStringValue(r.getAs("B_shipPostalCode")));
					B.setShipCountry(Util.getStringValue(r.getAs("B_shipCountry")));
					B.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("B_logEvents")));
						
					res.setOrderedProducts(A);
					res.setOrder(B);
					return res;
				}, Encoders.bean(ComposedOf.class));
		
		datasetsPOJO.add(res_Orders_order);
		
	
		
		Dataset<ComposedOf> res_composedOf_order;
		Dataset<Orders> res_Orders;
		
		
		//Join datasets or return 
		Dataset<ComposedOf> res = fullOuterJoinsComposedOf(datasetsPOJO);
		if(res == null)
			return null;
	
		Dataset<Orders> lonelyOrder = null;
		Dataset<Products> lonelyOrderedProducts = null;
		
	
	
		
		if(order_refilter.booleanValue() || orderedProducts_refilter.booleanValue() || composedOf_refilter.booleanValue())
			res = res.filter((FilterFunction<ComposedOf>) r -> (order_condition == null || order_condition.evaluate(r.getOrder())) && (orderedProducts_condition == null || orderedProducts_condition.evaluate(r.getOrderedProducts())) && (composedOf_condition == null || composedOf_condition.evaluate(r)));
		
	
		return res;
	
	}
	
	public Dataset<ComposedOf> getComposedOfListByOrderCondition(
		Condition<OrdersAttribute> order_condition
	){
		return getComposedOfList(order_condition, null, null);
	}
	
	public Dataset<ComposedOf> getComposedOfListByOrder(Orders order) {
		Condition<OrdersAttribute> cond = null;
		cond = Condition.simple(OrdersAttribute.id, Operator.EQUALS, order.getId());
		Dataset<ComposedOf> res = getComposedOfListByOrderCondition(cond);
	return res;
	}
	public Dataset<ComposedOf> getComposedOfListByOrderedProductsCondition(
		Condition<ProductsAttribute> orderedProducts_condition
	){
		return getComposedOfList(null, orderedProducts_condition, null);
	}
	
	public Dataset<ComposedOf> getComposedOfListByOrderedProducts(Products orderedProducts) {
		Condition<ProductsAttribute> cond = null;
		cond = Condition.simple(ProductsAttribute.productId, Operator.EQUALS, orderedProducts.getProductId());
		Dataset<ComposedOf> res = getComposedOfListByOrderedProductsCondition(cond);
	return res;
	}
	
	public Dataset<ComposedOf> getComposedOfListByComposedOfCondition(
		Condition<ComposedOfAttribute> composedOf_condition
	){
		return getComposedOfList(null, null, composedOf_condition);
	}
	
	public void insertComposedOf(ComposedOf composedOf){
		//Link entities in join structures.
		insertComposedOfInJoinStructOrder_DetailsInMyRelDB(composedOf);
		// Update embedded structures mapped to non mandatory roles.
		// Update ref fields mapped to non mandatory roles. 
	}
	
	public 	boolean insertComposedOfInJoinStructOrder_DetailsInMyRelDB(ComposedOf composedOf){
	 	// Rel 'composedOf' Insert in join structure 'Order_Details'
		
		Orders order_orders = composedOf.getOrder();
		Products orderedProducts_products = composedOf.getOrderedProducts();
		List<String> columns = new ArrayList<>();
		List<Object> values = new ArrayList<>();
		List<List<Object>> rows = new ArrayList<>();
		columns.add("UnitPrice");
		values.add(composedOf.getUnitPrice());
		columns.add("Quantity");
		values.add(composedOf.getQuantity());
		columns.add("Discount");
		values.add(composedOf.getDiscount());
		// Role in join structure 
		columns.add("OrderRef");
		Object ordersId = order_orders.getId();
		values.add(ordersId);
		// Role in join structure 
		columns.add("ProductRef");
		Object productsId = orderedProducts_products.getProductId();
		values.add(productsId);
		rows.add(values);
		DBConnectionMgr.insertInTable(columns, rows, "Order_Details", "myRelDB"); 					
		return true;
	
	}
	
	
	
	
	
	public void updateComposedOfList(
		conditions.Condition<conditions.OrdersAttribute> order_condition,
		conditions.Condition<conditions.ProductsAttribute> orderedProducts_condition,
		conditions.Condition<conditions.ComposedOfAttribute> composedOf_condition,
		conditions.SetClause<conditions.ComposedOfAttribute> set
	){
		//TODO
	}
	
	public void updateComposedOfListByOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> order_condition,
		conditions.SetClause<conditions.ComposedOfAttribute> set
	){
		updateComposedOfList(order_condition, null, null, set);
	}
	
	public void updateComposedOfListByOrder(pojo.Orders order, conditions.SetClause<conditions.ComposedOfAttribute> set) {
		// TODO using id for selecting
		return;
	}
	public void updateComposedOfListByOrderedProductsCondition(
		conditions.Condition<conditions.ProductsAttribute> orderedProducts_condition,
		conditions.SetClause<conditions.ComposedOfAttribute> set
	){
		updateComposedOfList(null, orderedProducts_condition, null, set);
	}
	
	public void updateComposedOfListByOrderedProducts(pojo.Products orderedProducts, conditions.SetClause<conditions.ComposedOfAttribute> set) {
		// TODO using id for selecting
		return;
	}
	
	public void updateComposedOfListByComposedOfCondition(
		conditions.Condition<conditions.ComposedOfAttribute> composedOf_condition,
		conditions.SetClause<conditions.ComposedOfAttribute> set
	){
		updateComposedOfList(null, null, composedOf_condition, set);
	}
	
	public void deleteComposedOfList(
		conditions.Condition<conditions.OrdersAttribute> order_condition,
		conditions.Condition<conditions.ProductsAttribute> orderedProducts_condition,
		conditions.Condition<conditions.ComposedOfAttribute> composedOf_condition){
			//TODO
		}
	
	public void deleteComposedOfListByOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> order_condition
	){
		deleteComposedOfList(order_condition, null, null);
	}
	
	public void deleteComposedOfListByOrder(pojo.Orders order) {
		// TODO using id for selecting
		return;
	}
	public void deleteComposedOfListByOrderedProductsCondition(
		conditions.Condition<conditions.ProductsAttribute> orderedProducts_condition
	){
		deleteComposedOfList(null, orderedProducts_condition, null);
	}
	
	public void deleteComposedOfListByOrderedProducts(pojo.Products orderedProducts) {
		// TODO using id for selecting
		return;
	}
	
	public void deleteComposedOfListByComposedOfCondition(
		conditions.Condition<conditions.ComposedOfAttribute> composedOf_condition
	){
		deleteComposedOfList(null, null, composedOf_condition);
	}
		
}
