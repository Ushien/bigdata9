package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import conditions.*;
import pojo.ComposedOf;
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


public abstract class ComposedOfService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ComposedOfService.class);
	// A<-AB->B . getAListInREL
	//join structure
	// Left side 'OrderRef' of reference [order ]
	public abstract Dataset<OrdersTDO> getOrdersTDOListOrderInOrderInOrdersFromMongoDB(Condition<OrdersAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	
	
	
	// A<-AB->B . getBListInREL
	
	
	public abstract Dataset<ComposedOfTDO> getComposedOfTDOListInProductsAndOrder_DetailsFrommyRelDB(Condition<ProductsAttribute> orderedProducts_cond, Condition<ComposedOfAttribute> composedOf_cond, MutableBoolean refilterFlag, MutableBoolean composedOf_refilter);
	
	
	
	
	
	
	public static Dataset<ComposedOf> fullLeftOuterJoinBetweenComposedOfAndOrder(Dataset<ComposedOf> d1, Dataset<Orders> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("id", "A_id")
			.withColumnRenamed("orderDate", "A_orderDate")
			.withColumnRenamed("requiredDate", "A_requiredDate")
			.withColumnRenamed("shippedDate", "A_shippedDate")
			.withColumnRenamed("freight", "A_freight")
			.withColumnRenamed("shipName", "A_shipName")
			.withColumnRenamed("shipAddress", "A_shipAddress")
			.withColumnRenamed("shipCity", "A_shipCity")
			.withColumnRenamed("shipRegion", "A_shipRegion")
			.withColumnRenamed("shipPostalCode", "A_shipPostalCode")
			.withColumnRenamed("shipCountry", "A_shipCountry")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("order.id").equalTo(d2_.col("A_id"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, ComposedOf>) r -> {
				ComposedOf res = new ComposedOf();
				res.setUnitPrice(r.getAs("unitPrice"));
				res.setQuantity(r.getAs("quantity"));
				res.setDiscount(r.getAs("discount"));
	
				Orders order = new Orders();
				Object o = r.getAs("order");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						order.setId(Util.getIntegerValue(r2.getAs("id")));
						order.setOrderDate(Util.getLocalDateValue(r2.getAs("orderDate")));
						order.setRequiredDate(Util.getLocalDateValue(r2.getAs("requiredDate")));
						order.setShippedDate(Util.getLocalDateValue(r2.getAs("shippedDate")));
						order.setFreight(Util.getDoubleValue(r2.getAs("freight")));
						order.setShipName(Util.getStringValue(r2.getAs("shipName")));
						order.setShipAddress(Util.getStringValue(r2.getAs("shipAddress")));
						order.setShipCity(Util.getStringValue(r2.getAs("shipCity")));
						order.setShipRegion(Util.getStringValue(r2.getAs("shipRegion")));
						order.setShipPostalCode(Util.getStringValue(r2.getAs("shipPostalCode")));
						order.setShipCountry(Util.getStringValue(r2.getAs("shipCountry")));
					} 
					if(o instanceof Orders) {
						order = (Orders) o;
					}
				}
	
				res.setOrder(order);
	
				Integer id = Util.getIntegerValue(r.getAs("A_id"));
				if (order.getId() != null && id != null && !order.getId().equals(id)) {
					res.addLogEvent("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.id': " + order.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.id': " + order.getId() + " and " + id + "." );
				}
				if(id != null)
					order.setId(id);
				LocalDate orderDate = Util.getLocalDateValue(r.getAs("A_orderDate"));
				if (order.getOrderDate() != null && orderDate != null && !order.getOrderDate().equals(orderDate)) {
					res.addLogEvent("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.orderDate': " + order.getOrderDate() + " and " + orderDate + "." );
					logger.warn("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.orderDate': " + order.getOrderDate() + " and " + orderDate + "." );
				}
				if(orderDate != null)
					order.setOrderDate(orderDate);
				LocalDate requiredDate = Util.getLocalDateValue(r.getAs("A_requiredDate"));
				if (order.getRequiredDate() != null && requiredDate != null && !order.getRequiredDate().equals(requiredDate)) {
					res.addLogEvent("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.requiredDate': " + order.getRequiredDate() + " and " + requiredDate + "." );
					logger.warn("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.requiredDate': " + order.getRequiredDate() + " and " + requiredDate + "." );
				}
				if(requiredDate != null)
					order.setRequiredDate(requiredDate);
				LocalDate shippedDate = Util.getLocalDateValue(r.getAs("A_shippedDate"));
				if (order.getShippedDate() != null && shippedDate != null && !order.getShippedDate().equals(shippedDate)) {
					res.addLogEvent("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.shippedDate': " + order.getShippedDate() + " and " + shippedDate + "." );
					logger.warn("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.shippedDate': " + order.getShippedDate() + " and " + shippedDate + "." );
				}
				if(shippedDate != null)
					order.setShippedDate(shippedDate);
				Double freight = Util.getDoubleValue(r.getAs("A_freight"));
				if (order.getFreight() != null && freight != null && !order.getFreight().equals(freight)) {
					res.addLogEvent("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.freight': " + order.getFreight() + " and " + freight + "." );
					logger.warn("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.freight': " + order.getFreight() + " and " + freight + "." );
				}
				if(freight != null)
					order.setFreight(freight);
				String shipName = Util.getStringValue(r.getAs("A_shipName"));
				if (order.getShipName() != null && shipName != null && !order.getShipName().equals(shipName)) {
					res.addLogEvent("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.shipName': " + order.getShipName() + " and " + shipName + "." );
					logger.warn("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.shipName': " + order.getShipName() + " and " + shipName + "." );
				}
				if(shipName != null)
					order.setShipName(shipName);
				String shipAddress = Util.getStringValue(r.getAs("A_shipAddress"));
				if (order.getShipAddress() != null && shipAddress != null && !order.getShipAddress().equals(shipAddress)) {
					res.addLogEvent("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.shipAddress': " + order.getShipAddress() + " and " + shipAddress + "." );
					logger.warn("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.shipAddress': " + order.getShipAddress() + " and " + shipAddress + "." );
				}
				if(shipAddress != null)
					order.setShipAddress(shipAddress);
				String shipCity = Util.getStringValue(r.getAs("A_shipCity"));
				if (order.getShipCity() != null && shipCity != null && !order.getShipCity().equals(shipCity)) {
					res.addLogEvent("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.shipCity': " + order.getShipCity() + " and " + shipCity + "." );
					logger.warn("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.shipCity': " + order.getShipCity() + " and " + shipCity + "." );
				}
				if(shipCity != null)
					order.setShipCity(shipCity);
				String shipRegion = Util.getStringValue(r.getAs("A_shipRegion"));
				if (order.getShipRegion() != null && shipRegion != null && !order.getShipRegion().equals(shipRegion)) {
					res.addLogEvent("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.shipRegion': " + order.getShipRegion() + " and " + shipRegion + "." );
					logger.warn("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.shipRegion': " + order.getShipRegion() + " and " + shipRegion + "." );
				}
				if(shipRegion != null)
					order.setShipRegion(shipRegion);
				String shipPostalCode = Util.getStringValue(r.getAs("A_shipPostalCode"));
				if (order.getShipPostalCode() != null && shipPostalCode != null && !order.getShipPostalCode().equals(shipPostalCode)) {
					res.addLogEvent("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.shipPostalCode': " + order.getShipPostalCode() + " and " + shipPostalCode + "." );
					logger.warn("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.shipPostalCode': " + order.getShipPostalCode() + " and " + shipPostalCode + "." );
				}
				if(shipPostalCode != null)
					order.setShipPostalCode(shipPostalCode);
				String shipCountry = Util.getStringValue(r.getAs("A_shipCountry"));
				if (order.getShipCountry() != null && shipCountry != null && !order.getShipCountry().equals(shipCountry)) {
					res.addLogEvent("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.shipCountry': " + order.getShipCountry() + " and " + shipCountry + "." );
					logger.warn("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.shipCountry': " + order.getShipCountry() + " and " + shipCountry + "." );
				}
				if(shipCountry != null)
					order.setShipCountry(shipCountry);
	
				o = r.getAs("orderedProducts");
				Products orderedProducts = new Products();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						orderedProducts.setProductId(Util.getIntegerValue(r2.getAs("productId")));
						orderedProducts.setProductName(Util.getStringValue(r2.getAs("productName")));
						orderedProducts.setQuantityPerUnit(Util.getStringValue(r2.getAs("quantityPerUnit")));
						orderedProducts.setUnitPrice(Util.getDoubleValue(r2.getAs("unitPrice")));
						orderedProducts.setUnitsInStock(Util.getIntegerValue(r2.getAs("unitsInStock")));
						orderedProducts.setUnitsOnOrder(Util.getIntegerValue(r2.getAs("unitsOnOrder")));
						orderedProducts.setReorderLevel(Util.getIntegerValue(r2.getAs("reorderLevel")));
						orderedProducts.setDiscontinued(Util.getBooleanValue(r2.getAs("discontinued")));
					} 
					if(o instanceof Products) {
						orderedProducts = (Products) o;
					}
				}
	
				res.setOrderedProducts(orderedProducts);
	
				return res;
		}, Encoders.bean(ComposedOf.class));
	
		
		
	}
	public static Dataset<ComposedOf> fullLeftOuterJoinBetweenComposedOfAndOrderedProducts(Dataset<ComposedOf> d1, Dataset<Products> d2) {
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
		joinCond = d1.col("orderedProducts.productId").equalTo(d2_.col("A_productId"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, ComposedOf>) r -> {
				ComposedOf res = new ComposedOf();
				res.setUnitPrice(r.getAs("unitPrice"));
				res.setQuantity(r.getAs("quantity"));
				res.setDiscount(r.getAs("discount"));
	
				Products orderedProducts = new Products();
				Object o = r.getAs("orderedProducts");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						orderedProducts.setProductId(Util.getIntegerValue(r2.getAs("productId")));
						orderedProducts.setProductName(Util.getStringValue(r2.getAs("productName")));
						orderedProducts.setQuantityPerUnit(Util.getStringValue(r2.getAs("quantityPerUnit")));
						orderedProducts.setUnitPrice(Util.getDoubleValue(r2.getAs("unitPrice")));
						orderedProducts.setUnitsInStock(Util.getIntegerValue(r2.getAs("unitsInStock")));
						orderedProducts.setUnitsOnOrder(Util.getIntegerValue(r2.getAs("unitsOnOrder")));
						orderedProducts.setReorderLevel(Util.getIntegerValue(r2.getAs("reorderLevel")));
						orderedProducts.setDiscontinued(Util.getBooleanValue(r2.getAs("discontinued")));
					} 
					if(o instanceof Products) {
						orderedProducts = (Products) o;
					}
				}
	
				res.setOrderedProducts(orderedProducts);
	
				Integer productId = Util.getIntegerValue(r.getAs("A_productId"));
				if (orderedProducts.getProductId() != null && productId != null && !orderedProducts.getProductId().equals(productId)) {
					res.addLogEvent("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.productId': " + orderedProducts.getProductId() + " and " + productId + "." );
					logger.warn("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.productId': " + orderedProducts.getProductId() + " and " + productId + "." );
				}
				if(productId != null)
					orderedProducts.setProductId(productId);
				String productName = Util.getStringValue(r.getAs("A_productName"));
				if (orderedProducts.getProductName() != null && productName != null && !orderedProducts.getProductName().equals(productName)) {
					res.addLogEvent("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.productName': " + orderedProducts.getProductName() + " and " + productName + "." );
					logger.warn("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.productName': " + orderedProducts.getProductName() + " and " + productName + "." );
				}
				if(productName != null)
					orderedProducts.setProductName(productName);
				String quantityPerUnit = Util.getStringValue(r.getAs("A_quantityPerUnit"));
				if (orderedProducts.getQuantityPerUnit() != null && quantityPerUnit != null && !orderedProducts.getQuantityPerUnit().equals(quantityPerUnit)) {
					res.addLogEvent("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.quantityPerUnit': " + orderedProducts.getQuantityPerUnit() + " and " + quantityPerUnit + "." );
					logger.warn("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.quantityPerUnit': " + orderedProducts.getQuantityPerUnit() + " and " + quantityPerUnit + "." );
				}
				if(quantityPerUnit != null)
					orderedProducts.setQuantityPerUnit(quantityPerUnit);
				Double unitPrice = Util.getDoubleValue(r.getAs("A_unitPrice"));
				if (orderedProducts.getUnitPrice() != null && unitPrice != null && !orderedProducts.getUnitPrice().equals(unitPrice)) {
					res.addLogEvent("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.unitPrice': " + orderedProducts.getUnitPrice() + " and " + unitPrice + "." );
					logger.warn("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.unitPrice': " + orderedProducts.getUnitPrice() + " and " + unitPrice + "." );
				}
				if(unitPrice != null)
					orderedProducts.setUnitPrice(unitPrice);
				Integer unitsInStock = Util.getIntegerValue(r.getAs("A_unitsInStock"));
				if (orderedProducts.getUnitsInStock() != null && unitsInStock != null && !orderedProducts.getUnitsInStock().equals(unitsInStock)) {
					res.addLogEvent("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.unitsInStock': " + orderedProducts.getUnitsInStock() + " and " + unitsInStock + "." );
					logger.warn("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.unitsInStock': " + orderedProducts.getUnitsInStock() + " and " + unitsInStock + "." );
				}
				if(unitsInStock != null)
					orderedProducts.setUnitsInStock(unitsInStock);
				Integer unitsOnOrder = Util.getIntegerValue(r.getAs("A_unitsOnOrder"));
				if (orderedProducts.getUnitsOnOrder() != null && unitsOnOrder != null && !orderedProducts.getUnitsOnOrder().equals(unitsOnOrder)) {
					res.addLogEvent("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.unitsOnOrder': " + orderedProducts.getUnitsOnOrder() + " and " + unitsOnOrder + "." );
					logger.warn("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.unitsOnOrder': " + orderedProducts.getUnitsOnOrder() + " and " + unitsOnOrder + "." );
				}
				if(unitsOnOrder != null)
					orderedProducts.setUnitsOnOrder(unitsOnOrder);
				Integer reorderLevel = Util.getIntegerValue(r.getAs("A_reorderLevel"));
				if (orderedProducts.getReorderLevel() != null && reorderLevel != null && !orderedProducts.getReorderLevel().equals(reorderLevel)) {
					res.addLogEvent("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.reorderLevel': " + orderedProducts.getReorderLevel() + " and " + reorderLevel + "." );
					logger.warn("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.reorderLevel': " + orderedProducts.getReorderLevel() + " and " + reorderLevel + "." );
				}
				if(reorderLevel != null)
					orderedProducts.setReorderLevel(reorderLevel);
				Boolean discontinued = Util.getBooleanValue(r.getAs("A_discontinued"));
				if (orderedProducts.getDiscontinued() != null && discontinued != null && !orderedProducts.getDiscontinued().equals(discontinued)) {
					res.addLogEvent("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.discontinued': " + orderedProducts.getDiscontinued() + " and " + discontinued + "." );
					logger.warn("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.discontinued': " + orderedProducts.getDiscontinued() + " and " + discontinued + "." );
				}
				if(discontinued != null)
					orderedProducts.setDiscontinued(discontinued);
	
				o = r.getAs("order");
				Orders order = new Orders();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						order.setId(Util.getIntegerValue(r2.getAs("id")));
						order.setOrderDate(Util.getLocalDateValue(r2.getAs("orderDate")));
						order.setRequiredDate(Util.getLocalDateValue(r2.getAs("requiredDate")));
						order.setShippedDate(Util.getLocalDateValue(r2.getAs("shippedDate")));
						order.setFreight(Util.getDoubleValue(r2.getAs("freight")));
						order.setShipName(Util.getStringValue(r2.getAs("shipName")));
						order.setShipAddress(Util.getStringValue(r2.getAs("shipAddress")));
						order.setShipCity(Util.getStringValue(r2.getAs("shipCity")));
						order.setShipRegion(Util.getStringValue(r2.getAs("shipRegion")));
						order.setShipPostalCode(Util.getStringValue(r2.getAs("shipPostalCode")));
						order.setShipCountry(Util.getStringValue(r2.getAs("shipCountry")));
					} 
					if(o instanceof Orders) {
						order = (Orders) o;
					}
				}
	
				res.setOrder(order);
	
				return res;
		}, Encoders.bean(ComposedOf.class));
	
		
		
	}
	
	public static Dataset<ComposedOf> fullOuterJoinsComposedOf(List<Dataset<ComposedOf>> datasetsPOJO) {
		return fullOuterJoinsComposedOf(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<ComposedOf> fullLeftOuterJoinsComposedOf(List<Dataset<ComposedOf>> datasetsPOJO) {
		return fullOuterJoinsComposedOf(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<ComposedOf> fullOuterJoinsComposedOf(List<Dataset<ComposedOf>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		List<String> idFields = new ArrayList<String>();
		idFields.add("order.id");
	
		idFields.add("orderedProducts.productId");
		scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
		
		List<Dataset<Row>> rows = new ArrayList<Dataset<Row>>();
		for(int i = 0; i < datasetsPOJO.size(); i++) {
			Dataset<ComposedOf> d = datasetsPOJO.get(i);
			rows.add(d
				.withColumn("order_id_" + i, d.col("order.id"))
				.withColumn("orderedProducts_productId_" + i, d.col("orderedProducts.productId"))
				.withColumnRenamed("unitPrice", "unitPrice_" + i)
				.withColumnRenamed("quantity", "quantity_" + i)
				.withColumnRenamed("discount", "discount_" + i)
				.withColumnRenamed("order", "order_" + i)
				.withColumnRenamed("orderedProducts", "orderedProducts_" + i)
				.withColumnRenamed("logEvents", "logEvents_" + i));
		}
		
		Column joinCond;
		joinCond = rows.get(0).col("order_id_0").equalTo(rows.get(1).col("order_id_1"));
		joinCond = joinCond.and(rows.get(0).col("orderedProducts_productId_0").equalTo(rows.get(1).col("orderedProducts_productId_1")));
		
		Dataset<Row> res = rows.get(0).join(rows.get(1), joinCond, joinMode);
		for(int i = 2; i < rows.size(); i++) {
			joinCond = rows.get(i - 1).col("order_id_" + (i - 1)).equalTo(rows.get(i).col("order_id_" + i));
			joinCond = joinCond.and(rows.get(i - 1).col("orderedProducts_productId_" + (i - 1)).equalTo(rows.get(i).col("orderedProducts_productId_" + i)));
			res = res.join(rows.get(i), joinCond, joinMode);
		}
	
		return res.map((MapFunction<Row, ComposedOf>) r -> {
				ComposedOf composedOf_res = new ComposedOf();
					
					// attribute 'ComposedOf.unitPrice'
					Double firstNotNull_unitPrice = Util.getDoubleValue(r.getAs("unitPrice_0"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double unitPrice2 = Util.getDoubleValue(r.getAs("unitPrice_" + i));
						if (firstNotNull_unitPrice != null && unitPrice2 != null && !firstNotNull_unitPrice.equals(unitPrice2)) {
							composedOf_res.addLogEvent("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.unitPrice': " + firstNotNull_unitPrice + " and " + unitPrice2 + "." );
							logger.warn("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.unitPrice': " + firstNotNull_unitPrice + " and " + unitPrice2 + "." );
						}
						if (firstNotNull_unitPrice == null && unitPrice2 != null) {
							firstNotNull_unitPrice = unitPrice2;
						}
					}
					composedOf_res.setUnitPrice(firstNotNull_unitPrice);
					
					// attribute 'ComposedOf.quantity'
					Integer firstNotNull_quantity = Util.getIntegerValue(r.getAs("quantity_0"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer quantity2 = Util.getIntegerValue(r.getAs("quantity_" + i));
						if (firstNotNull_quantity != null && quantity2 != null && !firstNotNull_quantity.equals(quantity2)) {
							composedOf_res.addLogEvent("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.quantity': " + firstNotNull_quantity + " and " + quantity2 + "." );
							logger.warn("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.quantity': " + firstNotNull_quantity + " and " + quantity2 + "." );
						}
						if (firstNotNull_quantity == null && quantity2 != null) {
							firstNotNull_quantity = quantity2;
						}
					}
					composedOf_res.setQuantity(firstNotNull_quantity);
					
					// attribute 'ComposedOf.discount'
					Double firstNotNull_discount = Util.getDoubleValue(r.getAs("discount_0"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double discount2 = Util.getDoubleValue(r.getAs("discount_" + i));
						if (firstNotNull_discount != null && discount2 != null && !firstNotNull_discount.equals(discount2)) {
							composedOf_res.addLogEvent("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.discount': " + firstNotNull_discount + " and " + discount2 + "." );
							logger.warn("Data consistency problem for [ComposedOf - different values found for attribute 'ComposedOf.discount': " + firstNotNull_discount + " and " + discount2 + "." );
						}
						if (firstNotNull_discount == null && discount2 != null) {
							firstNotNull_discount = discount2;
						}
					}
					composedOf_res.setDiscount(firstNotNull_discount);
	
					WrappedArray logEvents = r.getAs("logEvents_0");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							composedOf_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							composedOf_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					Orders order_res = new Orders();
					Products orderedProducts_res = new Products();
					
					// attribute 'Orders.id'
					Integer firstNotNull_order_id = Util.getIntegerValue(r.getAs("order_0.id"));
					order_res.setId(firstNotNull_order_id);
					// attribute 'Orders.orderDate'
					LocalDate firstNotNull_order_orderDate = Util.getLocalDateValue(r.getAs("order_0.orderDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate order_orderDate2 = Util.getLocalDateValue(r.getAs("order_" + i + ".orderDate"));
						if (firstNotNull_order_orderDate != null && order_orderDate2 != null && !firstNotNull_order_orderDate.equals(order_orderDate2)) {
							composedOf_res.addLogEvent("Data consistency problem for [Orders - id :"+order_res.getId()+"]: different values found for attribute 'Orders.orderDate': " + firstNotNull_order_orderDate + " and " + order_orderDate2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+order_res.getId()+"]: different values found for attribute 'Orders.orderDate': " + firstNotNull_order_orderDate + " and " + order_orderDate2 + "." );
						}
						if (firstNotNull_order_orderDate == null && order_orderDate2 != null) {
							firstNotNull_order_orderDate = order_orderDate2;
						}
					}
					order_res.setOrderDate(firstNotNull_order_orderDate);
					// attribute 'Orders.requiredDate'
					LocalDate firstNotNull_order_requiredDate = Util.getLocalDateValue(r.getAs("order_0.requiredDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate order_requiredDate2 = Util.getLocalDateValue(r.getAs("order_" + i + ".requiredDate"));
						if (firstNotNull_order_requiredDate != null && order_requiredDate2 != null && !firstNotNull_order_requiredDate.equals(order_requiredDate2)) {
							composedOf_res.addLogEvent("Data consistency problem for [Orders - id :"+order_res.getId()+"]: different values found for attribute 'Orders.requiredDate': " + firstNotNull_order_requiredDate + " and " + order_requiredDate2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+order_res.getId()+"]: different values found for attribute 'Orders.requiredDate': " + firstNotNull_order_requiredDate + " and " + order_requiredDate2 + "." );
						}
						if (firstNotNull_order_requiredDate == null && order_requiredDate2 != null) {
							firstNotNull_order_requiredDate = order_requiredDate2;
						}
					}
					order_res.setRequiredDate(firstNotNull_order_requiredDate);
					// attribute 'Orders.shippedDate'
					LocalDate firstNotNull_order_shippedDate = Util.getLocalDateValue(r.getAs("order_0.shippedDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate order_shippedDate2 = Util.getLocalDateValue(r.getAs("order_" + i + ".shippedDate"));
						if (firstNotNull_order_shippedDate != null && order_shippedDate2 != null && !firstNotNull_order_shippedDate.equals(order_shippedDate2)) {
							composedOf_res.addLogEvent("Data consistency problem for [Orders - id :"+order_res.getId()+"]: different values found for attribute 'Orders.shippedDate': " + firstNotNull_order_shippedDate + " and " + order_shippedDate2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+order_res.getId()+"]: different values found for attribute 'Orders.shippedDate': " + firstNotNull_order_shippedDate + " and " + order_shippedDate2 + "." );
						}
						if (firstNotNull_order_shippedDate == null && order_shippedDate2 != null) {
							firstNotNull_order_shippedDate = order_shippedDate2;
						}
					}
					order_res.setShippedDate(firstNotNull_order_shippedDate);
					// attribute 'Orders.freight'
					Double firstNotNull_order_freight = Util.getDoubleValue(r.getAs("order_0.freight"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double order_freight2 = Util.getDoubleValue(r.getAs("order_" + i + ".freight"));
						if (firstNotNull_order_freight != null && order_freight2 != null && !firstNotNull_order_freight.equals(order_freight2)) {
							composedOf_res.addLogEvent("Data consistency problem for [Orders - id :"+order_res.getId()+"]: different values found for attribute 'Orders.freight': " + firstNotNull_order_freight + " and " + order_freight2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+order_res.getId()+"]: different values found for attribute 'Orders.freight': " + firstNotNull_order_freight + " and " + order_freight2 + "." );
						}
						if (firstNotNull_order_freight == null && order_freight2 != null) {
							firstNotNull_order_freight = order_freight2;
						}
					}
					order_res.setFreight(firstNotNull_order_freight);
					// attribute 'Orders.shipName'
					String firstNotNull_order_shipName = Util.getStringValue(r.getAs("order_0.shipName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String order_shipName2 = Util.getStringValue(r.getAs("order_" + i + ".shipName"));
						if (firstNotNull_order_shipName != null && order_shipName2 != null && !firstNotNull_order_shipName.equals(order_shipName2)) {
							composedOf_res.addLogEvent("Data consistency problem for [Orders - id :"+order_res.getId()+"]: different values found for attribute 'Orders.shipName': " + firstNotNull_order_shipName + " and " + order_shipName2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+order_res.getId()+"]: different values found for attribute 'Orders.shipName': " + firstNotNull_order_shipName + " and " + order_shipName2 + "." );
						}
						if (firstNotNull_order_shipName == null && order_shipName2 != null) {
							firstNotNull_order_shipName = order_shipName2;
						}
					}
					order_res.setShipName(firstNotNull_order_shipName);
					// attribute 'Orders.shipAddress'
					String firstNotNull_order_shipAddress = Util.getStringValue(r.getAs("order_0.shipAddress"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String order_shipAddress2 = Util.getStringValue(r.getAs("order_" + i + ".shipAddress"));
						if (firstNotNull_order_shipAddress != null && order_shipAddress2 != null && !firstNotNull_order_shipAddress.equals(order_shipAddress2)) {
							composedOf_res.addLogEvent("Data consistency problem for [Orders - id :"+order_res.getId()+"]: different values found for attribute 'Orders.shipAddress': " + firstNotNull_order_shipAddress + " and " + order_shipAddress2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+order_res.getId()+"]: different values found for attribute 'Orders.shipAddress': " + firstNotNull_order_shipAddress + " and " + order_shipAddress2 + "." );
						}
						if (firstNotNull_order_shipAddress == null && order_shipAddress2 != null) {
							firstNotNull_order_shipAddress = order_shipAddress2;
						}
					}
					order_res.setShipAddress(firstNotNull_order_shipAddress);
					// attribute 'Orders.shipCity'
					String firstNotNull_order_shipCity = Util.getStringValue(r.getAs("order_0.shipCity"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String order_shipCity2 = Util.getStringValue(r.getAs("order_" + i + ".shipCity"));
						if (firstNotNull_order_shipCity != null && order_shipCity2 != null && !firstNotNull_order_shipCity.equals(order_shipCity2)) {
							composedOf_res.addLogEvent("Data consistency problem for [Orders - id :"+order_res.getId()+"]: different values found for attribute 'Orders.shipCity': " + firstNotNull_order_shipCity + " and " + order_shipCity2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+order_res.getId()+"]: different values found for attribute 'Orders.shipCity': " + firstNotNull_order_shipCity + " and " + order_shipCity2 + "." );
						}
						if (firstNotNull_order_shipCity == null && order_shipCity2 != null) {
							firstNotNull_order_shipCity = order_shipCity2;
						}
					}
					order_res.setShipCity(firstNotNull_order_shipCity);
					// attribute 'Orders.shipRegion'
					String firstNotNull_order_shipRegion = Util.getStringValue(r.getAs("order_0.shipRegion"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String order_shipRegion2 = Util.getStringValue(r.getAs("order_" + i + ".shipRegion"));
						if (firstNotNull_order_shipRegion != null && order_shipRegion2 != null && !firstNotNull_order_shipRegion.equals(order_shipRegion2)) {
							composedOf_res.addLogEvent("Data consistency problem for [Orders - id :"+order_res.getId()+"]: different values found for attribute 'Orders.shipRegion': " + firstNotNull_order_shipRegion + " and " + order_shipRegion2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+order_res.getId()+"]: different values found for attribute 'Orders.shipRegion': " + firstNotNull_order_shipRegion + " and " + order_shipRegion2 + "." );
						}
						if (firstNotNull_order_shipRegion == null && order_shipRegion2 != null) {
							firstNotNull_order_shipRegion = order_shipRegion2;
						}
					}
					order_res.setShipRegion(firstNotNull_order_shipRegion);
					// attribute 'Orders.shipPostalCode'
					String firstNotNull_order_shipPostalCode = Util.getStringValue(r.getAs("order_0.shipPostalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String order_shipPostalCode2 = Util.getStringValue(r.getAs("order_" + i + ".shipPostalCode"));
						if (firstNotNull_order_shipPostalCode != null && order_shipPostalCode2 != null && !firstNotNull_order_shipPostalCode.equals(order_shipPostalCode2)) {
							composedOf_res.addLogEvent("Data consistency problem for [Orders - id :"+order_res.getId()+"]: different values found for attribute 'Orders.shipPostalCode': " + firstNotNull_order_shipPostalCode + " and " + order_shipPostalCode2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+order_res.getId()+"]: different values found for attribute 'Orders.shipPostalCode': " + firstNotNull_order_shipPostalCode + " and " + order_shipPostalCode2 + "." );
						}
						if (firstNotNull_order_shipPostalCode == null && order_shipPostalCode2 != null) {
							firstNotNull_order_shipPostalCode = order_shipPostalCode2;
						}
					}
					order_res.setShipPostalCode(firstNotNull_order_shipPostalCode);
					// attribute 'Orders.shipCountry'
					String firstNotNull_order_shipCountry = Util.getStringValue(r.getAs("order_0.shipCountry"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String order_shipCountry2 = Util.getStringValue(r.getAs("order_" + i + ".shipCountry"));
						if (firstNotNull_order_shipCountry != null && order_shipCountry2 != null && !firstNotNull_order_shipCountry.equals(order_shipCountry2)) {
							composedOf_res.addLogEvent("Data consistency problem for [Orders - id :"+order_res.getId()+"]: different values found for attribute 'Orders.shipCountry': " + firstNotNull_order_shipCountry + " and " + order_shipCountry2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+order_res.getId()+"]: different values found for attribute 'Orders.shipCountry': " + firstNotNull_order_shipCountry + " and " + order_shipCountry2 + "." );
						}
						if (firstNotNull_order_shipCountry == null && order_shipCountry2 != null) {
							firstNotNull_order_shipCountry = order_shipCountry2;
						}
					}
					order_res.setShipCountry(firstNotNull_order_shipCountry);
					// attribute 'Products.productId'
					Integer firstNotNull_orderedProducts_productId = Util.getIntegerValue(r.getAs("orderedProducts_0.productId"));
					orderedProducts_res.setProductId(firstNotNull_orderedProducts_productId);
					// attribute 'Products.productName'
					String firstNotNull_orderedProducts_productName = Util.getStringValue(r.getAs("orderedProducts_0.productName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String orderedProducts_productName2 = Util.getStringValue(r.getAs("orderedProducts_" + i + ".productName"));
						if (firstNotNull_orderedProducts_productName != null && orderedProducts_productName2 != null && !firstNotNull_orderedProducts_productName.equals(orderedProducts_productName2)) {
							composedOf_res.addLogEvent("Data consistency problem for [Products - id :"+orderedProducts_res.getProductId()+"]: different values found for attribute 'Products.productName': " + firstNotNull_orderedProducts_productName + " and " + orderedProducts_productName2 + "." );
							logger.warn("Data consistency problem for [Products - id :"+orderedProducts_res.getProductId()+"]: different values found for attribute 'Products.productName': " + firstNotNull_orderedProducts_productName + " and " + orderedProducts_productName2 + "." );
						}
						if (firstNotNull_orderedProducts_productName == null && orderedProducts_productName2 != null) {
							firstNotNull_orderedProducts_productName = orderedProducts_productName2;
						}
					}
					orderedProducts_res.setProductName(firstNotNull_orderedProducts_productName);
					// attribute 'Products.quantityPerUnit'
					String firstNotNull_orderedProducts_quantityPerUnit = Util.getStringValue(r.getAs("orderedProducts_0.quantityPerUnit"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String orderedProducts_quantityPerUnit2 = Util.getStringValue(r.getAs("orderedProducts_" + i + ".quantityPerUnit"));
						if (firstNotNull_orderedProducts_quantityPerUnit != null && orderedProducts_quantityPerUnit2 != null && !firstNotNull_orderedProducts_quantityPerUnit.equals(orderedProducts_quantityPerUnit2)) {
							composedOf_res.addLogEvent("Data consistency problem for [Products - id :"+orderedProducts_res.getProductId()+"]: different values found for attribute 'Products.quantityPerUnit': " + firstNotNull_orderedProducts_quantityPerUnit + " and " + orderedProducts_quantityPerUnit2 + "." );
							logger.warn("Data consistency problem for [Products - id :"+orderedProducts_res.getProductId()+"]: different values found for attribute 'Products.quantityPerUnit': " + firstNotNull_orderedProducts_quantityPerUnit + " and " + orderedProducts_quantityPerUnit2 + "." );
						}
						if (firstNotNull_orderedProducts_quantityPerUnit == null && orderedProducts_quantityPerUnit2 != null) {
							firstNotNull_orderedProducts_quantityPerUnit = orderedProducts_quantityPerUnit2;
						}
					}
					orderedProducts_res.setQuantityPerUnit(firstNotNull_orderedProducts_quantityPerUnit);
					// attribute 'Products.unitPrice'
					Double firstNotNull_orderedProducts_unitPrice = Util.getDoubleValue(r.getAs("orderedProducts_0.unitPrice"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double orderedProducts_unitPrice2 = Util.getDoubleValue(r.getAs("orderedProducts_" + i + ".unitPrice"));
						if (firstNotNull_orderedProducts_unitPrice != null && orderedProducts_unitPrice2 != null && !firstNotNull_orderedProducts_unitPrice.equals(orderedProducts_unitPrice2)) {
							composedOf_res.addLogEvent("Data consistency problem for [Products - id :"+orderedProducts_res.getProductId()+"]: different values found for attribute 'Products.unitPrice': " + firstNotNull_orderedProducts_unitPrice + " and " + orderedProducts_unitPrice2 + "." );
							logger.warn("Data consistency problem for [Products - id :"+orderedProducts_res.getProductId()+"]: different values found for attribute 'Products.unitPrice': " + firstNotNull_orderedProducts_unitPrice + " and " + orderedProducts_unitPrice2 + "." );
						}
						if (firstNotNull_orderedProducts_unitPrice == null && orderedProducts_unitPrice2 != null) {
							firstNotNull_orderedProducts_unitPrice = orderedProducts_unitPrice2;
						}
					}
					orderedProducts_res.setUnitPrice(firstNotNull_orderedProducts_unitPrice);
					// attribute 'Products.unitsInStock'
					Integer firstNotNull_orderedProducts_unitsInStock = Util.getIntegerValue(r.getAs("orderedProducts_0.unitsInStock"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer orderedProducts_unitsInStock2 = Util.getIntegerValue(r.getAs("orderedProducts_" + i + ".unitsInStock"));
						if (firstNotNull_orderedProducts_unitsInStock != null && orderedProducts_unitsInStock2 != null && !firstNotNull_orderedProducts_unitsInStock.equals(orderedProducts_unitsInStock2)) {
							composedOf_res.addLogEvent("Data consistency problem for [Products - id :"+orderedProducts_res.getProductId()+"]: different values found for attribute 'Products.unitsInStock': " + firstNotNull_orderedProducts_unitsInStock + " and " + orderedProducts_unitsInStock2 + "." );
							logger.warn("Data consistency problem for [Products - id :"+orderedProducts_res.getProductId()+"]: different values found for attribute 'Products.unitsInStock': " + firstNotNull_orderedProducts_unitsInStock + " and " + orderedProducts_unitsInStock2 + "." );
						}
						if (firstNotNull_orderedProducts_unitsInStock == null && orderedProducts_unitsInStock2 != null) {
							firstNotNull_orderedProducts_unitsInStock = orderedProducts_unitsInStock2;
						}
					}
					orderedProducts_res.setUnitsInStock(firstNotNull_orderedProducts_unitsInStock);
					// attribute 'Products.unitsOnOrder'
					Integer firstNotNull_orderedProducts_unitsOnOrder = Util.getIntegerValue(r.getAs("orderedProducts_0.unitsOnOrder"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer orderedProducts_unitsOnOrder2 = Util.getIntegerValue(r.getAs("orderedProducts_" + i + ".unitsOnOrder"));
						if (firstNotNull_orderedProducts_unitsOnOrder != null && orderedProducts_unitsOnOrder2 != null && !firstNotNull_orderedProducts_unitsOnOrder.equals(orderedProducts_unitsOnOrder2)) {
							composedOf_res.addLogEvent("Data consistency problem for [Products - id :"+orderedProducts_res.getProductId()+"]: different values found for attribute 'Products.unitsOnOrder': " + firstNotNull_orderedProducts_unitsOnOrder + " and " + orderedProducts_unitsOnOrder2 + "." );
							logger.warn("Data consistency problem for [Products - id :"+orderedProducts_res.getProductId()+"]: different values found for attribute 'Products.unitsOnOrder': " + firstNotNull_orderedProducts_unitsOnOrder + " and " + orderedProducts_unitsOnOrder2 + "." );
						}
						if (firstNotNull_orderedProducts_unitsOnOrder == null && orderedProducts_unitsOnOrder2 != null) {
							firstNotNull_orderedProducts_unitsOnOrder = orderedProducts_unitsOnOrder2;
						}
					}
					orderedProducts_res.setUnitsOnOrder(firstNotNull_orderedProducts_unitsOnOrder);
					// attribute 'Products.reorderLevel'
					Integer firstNotNull_orderedProducts_reorderLevel = Util.getIntegerValue(r.getAs("orderedProducts_0.reorderLevel"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer orderedProducts_reorderLevel2 = Util.getIntegerValue(r.getAs("orderedProducts_" + i + ".reorderLevel"));
						if (firstNotNull_orderedProducts_reorderLevel != null && orderedProducts_reorderLevel2 != null && !firstNotNull_orderedProducts_reorderLevel.equals(orderedProducts_reorderLevel2)) {
							composedOf_res.addLogEvent("Data consistency problem for [Products - id :"+orderedProducts_res.getProductId()+"]: different values found for attribute 'Products.reorderLevel': " + firstNotNull_orderedProducts_reorderLevel + " and " + orderedProducts_reorderLevel2 + "." );
							logger.warn("Data consistency problem for [Products - id :"+orderedProducts_res.getProductId()+"]: different values found for attribute 'Products.reorderLevel': " + firstNotNull_orderedProducts_reorderLevel + " and " + orderedProducts_reorderLevel2 + "." );
						}
						if (firstNotNull_orderedProducts_reorderLevel == null && orderedProducts_reorderLevel2 != null) {
							firstNotNull_orderedProducts_reorderLevel = orderedProducts_reorderLevel2;
						}
					}
					orderedProducts_res.setReorderLevel(firstNotNull_orderedProducts_reorderLevel);
					// attribute 'Products.discontinued'
					Boolean firstNotNull_orderedProducts_discontinued = Util.getBooleanValue(r.getAs("orderedProducts_0.discontinued"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Boolean orderedProducts_discontinued2 = Util.getBooleanValue(r.getAs("orderedProducts_" + i + ".discontinued"));
						if (firstNotNull_orderedProducts_discontinued != null && orderedProducts_discontinued2 != null && !firstNotNull_orderedProducts_discontinued.equals(orderedProducts_discontinued2)) {
							composedOf_res.addLogEvent("Data consistency problem for [Products - id :"+orderedProducts_res.getProductId()+"]: different values found for attribute 'Products.discontinued': " + firstNotNull_orderedProducts_discontinued + " and " + orderedProducts_discontinued2 + "." );
							logger.warn("Data consistency problem for [Products - id :"+orderedProducts_res.getProductId()+"]: different values found for attribute 'Products.discontinued': " + firstNotNull_orderedProducts_discontinued + " and " + orderedProducts_discontinued2 + "." );
						}
						if (firstNotNull_orderedProducts_discontinued == null && orderedProducts_discontinued2 != null) {
							firstNotNull_orderedProducts_discontinued = orderedProducts_discontinued2;
						}
					}
					orderedProducts_res.setDiscontinued(firstNotNull_orderedProducts_discontinued);
	
					composedOf_res.setOrder(order_res);
					composedOf_res.setOrderedProducts(orderedProducts_res);
					return composedOf_res;
		}
		, Encoders.bean(ComposedOf.class));
	
	}
	
	//Empty arguments
	public Dataset<ComposedOf> getComposedOfList(){
		 return getComposedOfList(null,null,null);
	}
	
	public abstract Dataset<ComposedOf> getComposedOfList(
		Condition<OrdersAttribute> order_condition,
		Condition<ProductsAttribute> orderedProducts_condition,
		Condition<ComposedOfAttribute> composedOf_condition
	);
	
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
	
	public abstract void insertComposedOf(ComposedOf composedOf);
	
	public 	abstract boolean insertComposedOfInJoinStructOrder_DetailsInMyRelDB(ComposedOf composedOf);
	
	
	
	 public void insertComposedOf(Orders order ,Products orderedProducts ){
		ComposedOf composedOf = new ComposedOf();
		composedOf.setOrder(order);
		composedOf.setOrderedProducts(orderedProducts);
		insertComposedOf(composedOf);
	}
	
	 public void insertComposedOf(Products products, List<Orders> orderList){
		ComposedOf composedOf = new ComposedOf();
		composedOf.setOrderedProducts(products);
		for(Orders order : orderList){
			composedOf.setOrder(order);
			insertComposedOf(composedOf);
		}
	}
	 public void insertComposedOf(Orders orders, List<Products> orderedProductsList){
		ComposedOf composedOf = new ComposedOf();
		composedOf.setOrder(orders);
		for(Products orderedProducts : orderedProductsList){
			composedOf.setOrderedProducts(orderedProducts);
			insertComposedOf(composedOf);
		}
	}
	
	public abstract void updateComposedOfList(
		conditions.Condition<conditions.OrdersAttribute> order_condition,
		conditions.Condition<conditions.ProductsAttribute> orderedProducts_condition,
		conditions.Condition<conditions.ComposedOfAttribute> composedOf_condition,
		conditions.SetClause<conditions.ComposedOfAttribute> set
	);
	
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
	
	public abstract void deleteComposedOfList(
		conditions.Condition<conditions.OrdersAttribute> order_condition,
		conditions.Condition<conditions.ProductsAttribute> orderedProducts_condition,
		conditions.Condition<conditions.ComposedOfAttribute> composedOf_condition);
	
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
