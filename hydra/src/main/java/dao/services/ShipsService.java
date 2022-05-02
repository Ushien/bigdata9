package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import conditions.*;
import pojo.Ships;
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


public abstract class ShipsService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ShipsService.class);
	
	// method accessing the embedded object ShipmentInfo mapped to role shippedOrder
	public abstract Dataset<Ships> getShipsListInmongoDBOrdersShipmentInfo(Condition<OrdersAttribute> shippedOrder_condition, Condition<ShippersAttribute> shipper_condition, MutableBoolean shippedOrder_refilter, MutableBoolean shipper_refilter);
	
	
	public static Dataset<Ships> fullLeftOuterJoinBetweenShipsAndShippedOrder(Dataset<Ships> d1, Dataset<Orders> d2) {
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
		joinCond = d1.col("shippedOrder.id").equalTo(d2_.col("A_id"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Ships>) r -> {
				Ships res = new Ships();
	
				Orders shippedOrder = new Orders();
				Object o = r.getAs("shippedOrder");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						shippedOrder.setId(Util.getIntegerValue(r2.getAs("id")));
						shippedOrder.setOrderDate(Util.getLocalDateValue(r2.getAs("orderDate")));
						shippedOrder.setRequiredDate(Util.getLocalDateValue(r2.getAs("requiredDate")));
						shippedOrder.setShippedDate(Util.getLocalDateValue(r2.getAs("shippedDate")));
						shippedOrder.setFreight(Util.getDoubleValue(r2.getAs("freight")));
						shippedOrder.setShipName(Util.getStringValue(r2.getAs("shipName")));
						shippedOrder.setShipAddress(Util.getStringValue(r2.getAs("shipAddress")));
						shippedOrder.setShipCity(Util.getStringValue(r2.getAs("shipCity")));
						shippedOrder.setShipRegion(Util.getStringValue(r2.getAs("shipRegion")));
						shippedOrder.setShipPostalCode(Util.getStringValue(r2.getAs("shipPostalCode")));
						shippedOrder.setShipCountry(Util.getStringValue(r2.getAs("shipCountry")));
					} 
					if(o instanceof Orders) {
						shippedOrder = (Orders) o;
					}
				}
	
				res.setShippedOrder(shippedOrder);
	
				Integer id = Util.getIntegerValue(r.getAs("A_id"));
				if (shippedOrder.getId() != null && id != null && !shippedOrder.getId().equals(id)) {
					res.addLogEvent("Data consistency problem for [Ships - different values found for attribute 'Ships.id': " + shippedOrder.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Ships - different values found for attribute 'Ships.id': " + shippedOrder.getId() + " and " + id + "." );
				}
				if(id != null)
					shippedOrder.setId(id);
				LocalDate orderDate = Util.getLocalDateValue(r.getAs("A_orderDate"));
				if (shippedOrder.getOrderDate() != null && orderDate != null && !shippedOrder.getOrderDate().equals(orderDate)) {
					res.addLogEvent("Data consistency problem for [Ships - different values found for attribute 'Ships.orderDate': " + shippedOrder.getOrderDate() + " and " + orderDate + "." );
					logger.warn("Data consistency problem for [Ships - different values found for attribute 'Ships.orderDate': " + shippedOrder.getOrderDate() + " and " + orderDate + "." );
				}
				if(orderDate != null)
					shippedOrder.setOrderDate(orderDate);
				LocalDate requiredDate = Util.getLocalDateValue(r.getAs("A_requiredDate"));
				if (shippedOrder.getRequiredDate() != null && requiredDate != null && !shippedOrder.getRequiredDate().equals(requiredDate)) {
					res.addLogEvent("Data consistency problem for [Ships - different values found for attribute 'Ships.requiredDate': " + shippedOrder.getRequiredDate() + " and " + requiredDate + "." );
					logger.warn("Data consistency problem for [Ships - different values found for attribute 'Ships.requiredDate': " + shippedOrder.getRequiredDate() + " and " + requiredDate + "." );
				}
				if(requiredDate != null)
					shippedOrder.setRequiredDate(requiredDate);
				LocalDate shippedDate = Util.getLocalDateValue(r.getAs("A_shippedDate"));
				if (shippedOrder.getShippedDate() != null && shippedDate != null && !shippedOrder.getShippedDate().equals(shippedDate)) {
					res.addLogEvent("Data consistency problem for [Ships - different values found for attribute 'Ships.shippedDate': " + shippedOrder.getShippedDate() + " and " + shippedDate + "." );
					logger.warn("Data consistency problem for [Ships - different values found for attribute 'Ships.shippedDate': " + shippedOrder.getShippedDate() + " and " + shippedDate + "." );
				}
				if(shippedDate != null)
					shippedOrder.setShippedDate(shippedDate);
				Double freight = Util.getDoubleValue(r.getAs("A_freight"));
				if (shippedOrder.getFreight() != null && freight != null && !shippedOrder.getFreight().equals(freight)) {
					res.addLogEvent("Data consistency problem for [Ships - different values found for attribute 'Ships.freight': " + shippedOrder.getFreight() + " and " + freight + "." );
					logger.warn("Data consistency problem for [Ships - different values found for attribute 'Ships.freight': " + shippedOrder.getFreight() + " and " + freight + "." );
				}
				if(freight != null)
					shippedOrder.setFreight(freight);
				String shipName = Util.getStringValue(r.getAs("A_shipName"));
				if (shippedOrder.getShipName() != null && shipName != null && !shippedOrder.getShipName().equals(shipName)) {
					res.addLogEvent("Data consistency problem for [Ships - different values found for attribute 'Ships.shipName': " + shippedOrder.getShipName() + " and " + shipName + "." );
					logger.warn("Data consistency problem for [Ships - different values found for attribute 'Ships.shipName': " + shippedOrder.getShipName() + " and " + shipName + "." );
				}
				if(shipName != null)
					shippedOrder.setShipName(shipName);
				String shipAddress = Util.getStringValue(r.getAs("A_shipAddress"));
				if (shippedOrder.getShipAddress() != null && shipAddress != null && !shippedOrder.getShipAddress().equals(shipAddress)) {
					res.addLogEvent("Data consistency problem for [Ships - different values found for attribute 'Ships.shipAddress': " + shippedOrder.getShipAddress() + " and " + shipAddress + "." );
					logger.warn("Data consistency problem for [Ships - different values found for attribute 'Ships.shipAddress': " + shippedOrder.getShipAddress() + " and " + shipAddress + "." );
				}
				if(shipAddress != null)
					shippedOrder.setShipAddress(shipAddress);
				String shipCity = Util.getStringValue(r.getAs("A_shipCity"));
				if (shippedOrder.getShipCity() != null && shipCity != null && !shippedOrder.getShipCity().equals(shipCity)) {
					res.addLogEvent("Data consistency problem for [Ships - different values found for attribute 'Ships.shipCity': " + shippedOrder.getShipCity() + " and " + shipCity + "." );
					logger.warn("Data consistency problem for [Ships - different values found for attribute 'Ships.shipCity': " + shippedOrder.getShipCity() + " and " + shipCity + "." );
				}
				if(shipCity != null)
					shippedOrder.setShipCity(shipCity);
				String shipRegion = Util.getStringValue(r.getAs("A_shipRegion"));
				if (shippedOrder.getShipRegion() != null && shipRegion != null && !shippedOrder.getShipRegion().equals(shipRegion)) {
					res.addLogEvent("Data consistency problem for [Ships - different values found for attribute 'Ships.shipRegion': " + shippedOrder.getShipRegion() + " and " + shipRegion + "." );
					logger.warn("Data consistency problem for [Ships - different values found for attribute 'Ships.shipRegion': " + shippedOrder.getShipRegion() + " and " + shipRegion + "." );
				}
				if(shipRegion != null)
					shippedOrder.setShipRegion(shipRegion);
				String shipPostalCode = Util.getStringValue(r.getAs("A_shipPostalCode"));
				if (shippedOrder.getShipPostalCode() != null && shipPostalCode != null && !shippedOrder.getShipPostalCode().equals(shipPostalCode)) {
					res.addLogEvent("Data consistency problem for [Ships - different values found for attribute 'Ships.shipPostalCode': " + shippedOrder.getShipPostalCode() + " and " + shipPostalCode + "." );
					logger.warn("Data consistency problem for [Ships - different values found for attribute 'Ships.shipPostalCode': " + shippedOrder.getShipPostalCode() + " and " + shipPostalCode + "." );
				}
				if(shipPostalCode != null)
					shippedOrder.setShipPostalCode(shipPostalCode);
				String shipCountry = Util.getStringValue(r.getAs("A_shipCountry"));
				if (shippedOrder.getShipCountry() != null && shipCountry != null && !shippedOrder.getShipCountry().equals(shipCountry)) {
					res.addLogEvent("Data consistency problem for [Ships - different values found for attribute 'Ships.shipCountry': " + shippedOrder.getShipCountry() + " and " + shipCountry + "." );
					logger.warn("Data consistency problem for [Ships - different values found for attribute 'Ships.shipCountry': " + shippedOrder.getShipCountry() + " and " + shipCountry + "." );
				}
				if(shipCountry != null)
					shippedOrder.setShipCountry(shipCountry);
	
				o = r.getAs("shipper");
				Shippers shipper = new Shippers();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						shipper.setShipperID(Util.getIntegerValue(r2.getAs("shipperID")));
						shipper.setCompanyName(Util.getStringValue(r2.getAs("companyName")));
						shipper.setPhone(Util.getStringValue(r2.getAs("phone")));
					} 
					if(o instanceof Shippers) {
						shipper = (Shippers) o;
					}
				}
	
				res.setShipper(shipper);
	
				return res;
		}, Encoders.bean(Ships.class));
	
		
		
	}
	public static Dataset<Ships> fullLeftOuterJoinBetweenShipsAndShipper(Dataset<Ships> d1, Dataset<Shippers> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("shipperID", "A_shipperID")
			.withColumnRenamed("companyName", "A_companyName")
			.withColumnRenamed("phone", "A_phone")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("shipper.shipperID").equalTo(d2_.col("A_shipperID"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Ships>) r -> {
				Ships res = new Ships();
	
				Shippers shipper = new Shippers();
				Object o = r.getAs("shipper");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						shipper.setShipperID(Util.getIntegerValue(r2.getAs("shipperID")));
						shipper.setCompanyName(Util.getStringValue(r2.getAs("companyName")));
						shipper.setPhone(Util.getStringValue(r2.getAs("phone")));
					} 
					if(o instanceof Shippers) {
						shipper = (Shippers) o;
					}
				}
	
				res.setShipper(shipper);
	
				Integer shipperID = Util.getIntegerValue(r.getAs("A_shipperID"));
				if (shipper.getShipperID() != null && shipperID != null && !shipper.getShipperID().equals(shipperID)) {
					res.addLogEvent("Data consistency problem for [Ships - different values found for attribute 'Ships.shipperID': " + shipper.getShipperID() + " and " + shipperID + "." );
					logger.warn("Data consistency problem for [Ships - different values found for attribute 'Ships.shipperID': " + shipper.getShipperID() + " and " + shipperID + "." );
				}
				if(shipperID != null)
					shipper.setShipperID(shipperID);
				String companyName = Util.getStringValue(r.getAs("A_companyName"));
				if (shipper.getCompanyName() != null && companyName != null && !shipper.getCompanyName().equals(companyName)) {
					res.addLogEvent("Data consistency problem for [Ships - different values found for attribute 'Ships.companyName': " + shipper.getCompanyName() + " and " + companyName + "." );
					logger.warn("Data consistency problem for [Ships - different values found for attribute 'Ships.companyName': " + shipper.getCompanyName() + " and " + companyName + "." );
				}
				if(companyName != null)
					shipper.setCompanyName(companyName);
				String phone = Util.getStringValue(r.getAs("A_phone"));
				if (shipper.getPhone() != null && phone != null && !shipper.getPhone().equals(phone)) {
					res.addLogEvent("Data consistency problem for [Ships - different values found for attribute 'Ships.phone': " + shipper.getPhone() + " and " + phone + "." );
					logger.warn("Data consistency problem for [Ships - different values found for attribute 'Ships.phone': " + shipper.getPhone() + " and " + phone + "." );
				}
				if(phone != null)
					shipper.setPhone(phone);
	
				o = r.getAs("shippedOrder");
				Orders shippedOrder = new Orders();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						shippedOrder.setId(Util.getIntegerValue(r2.getAs("id")));
						shippedOrder.setOrderDate(Util.getLocalDateValue(r2.getAs("orderDate")));
						shippedOrder.setRequiredDate(Util.getLocalDateValue(r2.getAs("requiredDate")));
						shippedOrder.setShippedDate(Util.getLocalDateValue(r2.getAs("shippedDate")));
						shippedOrder.setFreight(Util.getDoubleValue(r2.getAs("freight")));
						shippedOrder.setShipName(Util.getStringValue(r2.getAs("shipName")));
						shippedOrder.setShipAddress(Util.getStringValue(r2.getAs("shipAddress")));
						shippedOrder.setShipCity(Util.getStringValue(r2.getAs("shipCity")));
						shippedOrder.setShipRegion(Util.getStringValue(r2.getAs("shipRegion")));
						shippedOrder.setShipPostalCode(Util.getStringValue(r2.getAs("shipPostalCode")));
						shippedOrder.setShipCountry(Util.getStringValue(r2.getAs("shipCountry")));
					} 
					if(o instanceof Orders) {
						shippedOrder = (Orders) o;
					}
				}
	
				res.setShippedOrder(shippedOrder);
	
				return res;
		}, Encoders.bean(Ships.class));
	
		
		
	}
	
	public static Dataset<Ships> fullOuterJoinsShips(List<Dataset<Ships>> datasetsPOJO) {
		return fullOuterJoinsShips(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Ships> fullLeftOuterJoinsShips(List<Dataset<Ships>> datasetsPOJO) {
		return fullOuterJoinsShips(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Ships> fullOuterJoinsShips(List<Dataset<Ships>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		List<String> idFields = new ArrayList<String>();
		idFields.add("shippedOrder.id");
	
		idFields.add("shipper.shipperID");
		scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
		
		List<Dataset<Row>> rows = new ArrayList<Dataset<Row>>();
		for(int i = 0; i < datasetsPOJO.size(); i++) {
			Dataset<Ships> d = datasetsPOJO.get(i);
			rows.add(d
				.withColumn("shippedOrder_id_" + i, d.col("shippedOrder.id"))
				.withColumn("shipper_shipperID_" + i, d.col("shipper.shipperID"))
				.withColumnRenamed("shippedOrder", "shippedOrder_" + i)
				.withColumnRenamed("shipper", "shipper_" + i)
				.withColumnRenamed("logEvents", "logEvents_" + i));
		}
		
		Column joinCond;
		joinCond = rows.get(0).col("shippedOrder_id_0").equalTo(rows.get(1).col("shippedOrder_id_1"));
		joinCond = joinCond.and(rows.get(0).col("shipper_shipperID_0").equalTo(rows.get(1).col("shipper_shipperID_1")));
		
		Dataset<Row> res = rows.get(0).join(rows.get(1), joinCond, joinMode);
		for(int i = 2; i < rows.size(); i++) {
			joinCond = rows.get(i - 1).col("shippedOrder_id_" + (i - 1)).equalTo(rows.get(i).col("shippedOrder_id_" + i));
			joinCond = joinCond.and(rows.get(i - 1).col("shipper_shipperID_" + (i - 1)).equalTo(rows.get(i).col("shipper_shipperID_" + i)));
			res = res.join(rows.get(i), joinCond, joinMode);
		}
	
		return res.map((MapFunction<Row, Ships>) r -> {
				Ships ships_res = new Ships();
	
					WrappedArray logEvents = r.getAs("logEvents_0");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							ships_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							ships_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					Orders shippedOrder_res = new Orders();
					Shippers shipper_res = new Shippers();
					
					// attribute 'Orders.id'
					Integer firstNotNull_shippedOrder_id = Util.getIntegerValue(r.getAs("shippedOrder_0.id"));
					shippedOrder_res.setId(firstNotNull_shippedOrder_id);
					// attribute 'Orders.orderDate'
					LocalDate firstNotNull_shippedOrder_OrderDate = Util.getLocalDateValue(r.getAs("shippedOrder_0.orderDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate shippedOrder_orderDate2 = Util.getLocalDateValue(r.getAs("shippedOrder_" + i + ".orderDate"));
						if (firstNotNull_shippedOrder_OrderDate != null && shippedOrder_orderDate2 != null && !firstNotNull_shippedOrder_OrderDate.equals(shippedOrder_orderDate2)) {
							ships_res.addLogEvent("Data consistency problem for [Orders - id :"+shippedOrder_res.getId()+"]: different values found for attribute 'Orders.orderDate': " + firstNotNull_shippedOrder_OrderDate + " and " + shippedOrder_orderDate2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+shippedOrder_res.getId()+"]: different values found for attribute 'Orders.orderDate': " + firstNotNull_shippedOrder_OrderDate + " and " + shippedOrder_orderDate2 + "." );
						}
						if (firstNotNull_shippedOrder_OrderDate == null && shippedOrder_orderDate2 != null) {
							firstNotNull_shippedOrder_OrderDate = shippedOrder_orderDate2;
						}
					}
					shippedOrder_res.setOrderDate(firstNotNull_shippedOrder_OrderDate);
					// attribute 'Orders.requiredDate'
					LocalDate firstNotNull_shippedOrder_RequiredDate = Util.getLocalDateValue(r.getAs("shippedOrder_0.requiredDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate shippedOrder_requiredDate2 = Util.getLocalDateValue(r.getAs("shippedOrder_" + i + ".requiredDate"));
						if (firstNotNull_shippedOrder_RequiredDate != null && shippedOrder_requiredDate2 != null && !firstNotNull_shippedOrder_RequiredDate.equals(shippedOrder_requiredDate2)) {
							ships_res.addLogEvent("Data consistency problem for [Orders - id :"+shippedOrder_res.getId()+"]: different values found for attribute 'Orders.requiredDate': " + firstNotNull_shippedOrder_RequiredDate + " and " + shippedOrder_requiredDate2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+shippedOrder_res.getId()+"]: different values found for attribute 'Orders.requiredDate': " + firstNotNull_shippedOrder_RequiredDate + " and " + shippedOrder_requiredDate2 + "." );
						}
						if (firstNotNull_shippedOrder_RequiredDate == null && shippedOrder_requiredDate2 != null) {
							firstNotNull_shippedOrder_RequiredDate = shippedOrder_requiredDate2;
						}
					}
					shippedOrder_res.setRequiredDate(firstNotNull_shippedOrder_RequiredDate);
					// attribute 'Orders.shippedDate'
					LocalDate firstNotNull_shippedOrder_ShippedDate = Util.getLocalDateValue(r.getAs("shippedOrder_0.shippedDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate shippedOrder_shippedDate2 = Util.getLocalDateValue(r.getAs("shippedOrder_" + i + ".shippedDate"));
						if (firstNotNull_shippedOrder_ShippedDate != null && shippedOrder_shippedDate2 != null && !firstNotNull_shippedOrder_ShippedDate.equals(shippedOrder_shippedDate2)) {
							ships_res.addLogEvent("Data consistency problem for [Orders - id :"+shippedOrder_res.getId()+"]: different values found for attribute 'Orders.shippedDate': " + firstNotNull_shippedOrder_ShippedDate + " and " + shippedOrder_shippedDate2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+shippedOrder_res.getId()+"]: different values found for attribute 'Orders.shippedDate': " + firstNotNull_shippedOrder_ShippedDate + " and " + shippedOrder_shippedDate2 + "." );
						}
						if (firstNotNull_shippedOrder_ShippedDate == null && shippedOrder_shippedDate2 != null) {
							firstNotNull_shippedOrder_ShippedDate = shippedOrder_shippedDate2;
						}
					}
					shippedOrder_res.setShippedDate(firstNotNull_shippedOrder_ShippedDate);
					// attribute 'Orders.freight'
					Double firstNotNull_shippedOrder_Freight = Util.getDoubleValue(r.getAs("shippedOrder_0.freight"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double shippedOrder_freight2 = Util.getDoubleValue(r.getAs("shippedOrder_" + i + ".freight"));
						if (firstNotNull_shippedOrder_Freight != null && shippedOrder_freight2 != null && !firstNotNull_shippedOrder_Freight.equals(shippedOrder_freight2)) {
							ships_res.addLogEvent("Data consistency problem for [Orders - id :"+shippedOrder_res.getId()+"]: different values found for attribute 'Orders.freight': " + firstNotNull_shippedOrder_Freight + " and " + shippedOrder_freight2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+shippedOrder_res.getId()+"]: different values found for attribute 'Orders.freight': " + firstNotNull_shippedOrder_Freight + " and " + shippedOrder_freight2 + "." );
						}
						if (firstNotNull_shippedOrder_Freight == null && shippedOrder_freight2 != null) {
							firstNotNull_shippedOrder_Freight = shippedOrder_freight2;
						}
					}
					shippedOrder_res.setFreight(firstNotNull_shippedOrder_Freight);
					// attribute 'Orders.shipName'
					String firstNotNull_shippedOrder_ShipName = Util.getStringValue(r.getAs("shippedOrder_0.shipName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String shippedOrder_shipName2 = Util.getStringValue(r.getAs("shippedOrder_" + i + ".shipName"));
						if (firstNotNull_shippedOrder_ShipName != null && shippedOrder_shipName2 != null && !firstNotNull_shippedOrder_ShipName.equals(shippedOrder_shipName2)) {
							ships_res.addLogEvent("Data consistency problem for [Orders - id :"+shippedOrder_res.getId()+"]: different values found for attribute 'Orders.shipName': " + firstNotNull_shippedOrder_ShipName + " and " + shippedOrder_shipName2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+shippedOrder_res.getId()+"]: different values found for attribute 'Orders.shipName': " + firstNotNull_shippedOrder_ShipName + " and " + shippedOrder_shipName2 + "." );
						}
						if (firstNotNull_shippedOrder_ShipName == null && shippedOrder_shipName2 != null) {
							firstNotNull_shippedOrder_ShipName = shippedOrder_shipName2;
						}
					}
					shippedOrder_res.setShipName(firstNotNull_shippedOrder_ShipName);
					// attribute 'Orders.shipAddress'
					String firstNotNull_shippedOrder_ShipAddress = Util.getStringValue(r.getAs("shippedOrder_0.shipAddress"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String shippedOrder_shipAddress2 = Util.getStringValue(r.getAs("shippedOrder_" + i + ".shipAddress"));
						if (firstNotNull_shippedOrder_ShipAddress != null && shippedOrder_shipAddress2 != null && !firstNotNull_shippedOrder_ShipAddress.equals(shippedOrder_shipAddress2)) {
							ships_res.addLogEvent("Data consistency problem for [Orders - id :"+shippedOrder_res.getId()+"]: different values found for attribute 'Orders.shipAddress': " + firstNotNull_shippedOrder_ShipAddress + " and " + shippedOrder_shipAddress2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+shippedOrder_res.getId()+"]: different values found for attribute 'Orders.shipAddress': " + firstNotNull_shippedOrder_ShipAddress + " and " + shippedOrder_shipAddress2 + "." );
						}
						if (firstNotNull_shippedOrder_ShipAddress == null && shippedOrder_shipAddress2 != null) {
							firstNotNull_shippedOrder_ShipAddress = shippedOrder_shipAddress2;
						}
					}
					shippedOrder_res.setShipAddress(firstNotNull_shippedOrder_ShipAddress);
					// attribute 'Orders.shipCity'
					String firstNotNull_shippedOrder_ShipCity = Util.getStringValue(r.getAs("shippedOrder_0.shipCity"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String shippedOrder_shipCity2 = Util.getStringValue(r.getAs("shippedOrder_" + i + ".shipCity"));
						if (firstNotNull_shippedOrder_ShipCity != null && shippedOrder_shipCity2 != null && !firstNotNull_shippedOrder_ShipCity.equals(shippedOrder_shipCity2)) {
							ships_res.addLogEvent("Data consistency problem for [Orders - id :"+shippedOrder_res.getId()+"]: different values found for attribute 'Orders.shipCity': " + firstNotNull_shippedOrder_ShipCity + " and " + shippedOrder_shipCity2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+shippedOrder_res.getId()+"]: different values found for attribute 'Orders.shipCity': " + firstNotNull_shippedOrder_ShipCity + " and " + shippedOrder_shipCity2 + "." );
						}
						if (firstNotNull_shippedOrder_ShipCity == null && shippedOrder_shipCity2 != null) {
							firstNotNull_shippedOrder_ShipCity = shippedOrder_shipCity2;
						}
					}
					shippedOrder_res.setShipCity(firstNotNull_shippedOrder_ShipCity);
					// attribute 'Orders.shipRegion'
					String firstNotNull_shippedOrder_ShipRegion = Util.getStringValue(r.getAs("shippedOrder_0.shipRegion"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String shippedOrder_shipRegion2 = Util.getStringValue(r.getAs("shippedOrder_" + i + ".shipRegion"));
						if (firstNotNull_shippedOrder_ShipRegion != null && shippedOrder_shipRegion2 != null && !firstNotNull_shippedOrder_ShipRegion.equals(shippedOrder_shipRegion2)) {
							ships_res.addLogEvent("Data consistency problem for [Orders - id :"+shippedOrder_res.getId()+"]: different values found for attribute 'Orders.shipRegion': " + firstNotNull_shippedOrder_ShipRegion + " and " + shippedOrder_shipRegion2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+shippedOrder_res.getId()+"]: different values found for attribute 'Orders.shipRegion': " + firstNotNull_shippedOrder_ShipRegion + " and " + shippedOrder_shipRegion2 + "." );
						}
						if (firstNotNull_shippedOrder_ShipRegion == null && shippedOrder_shipRegion2 != null) {
							firstNotNull_shippedOrder_ShipRegion = shippedOrder_shipRegion2;
						}
					}
					shippedOrder_res.setShipRegion(firstNotNull_shippedOrder_ShipRegion);
					// attribute 'Orders.shipPostalCode'
					String firstNotNull_shippedOrder_ShipPostalCode = Util.getStringValue(r.getAs("shippedOrder_0.shipPostalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String shippedOrder_shipPostalCode2 = Util.getStringValue(r.getAs("shippedOrder_" + i + ".shipPostalCode"));
						if (firstNotNull_shippedOrder_ShipPostalCode != null && shippedOrder_shipPostalCode2 != null && !firstNotNull_shippedOrder_ShipPostalCode.equals(shippedOrder_shipPostalCode2)) {
							ships_res.addLogEvent("Data consistency problem for [Orders - id :"+shippedOrder_res.getId()+"]: different values found for attribute 'Orders.shipPostalCode': " + firstNotNull_shippedOrder_ShipPostalCode + " and " + shippedOrder_shipPostalCode2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+shippedOrder_res.getId()+"]: different values found for attribute 'Orders.shipPostalCode': " + firstNotNull_shippedOrder_ShipPostalCode + " and " + shippedOrder_shipPostalCode2 + "." );
						}
						if (firstNotNull_shippedOrder_ShipPostalCode == null && shippedOrder_shipPostalCode2 != null) {
							firstNotNull_shippedOrder_ShipPostalCode = shippedOrder_shipPostalCode2;
						}
					}
					shippedOrder_res.setShipPostalCode(firstNotNull_shippedOrder_ShipPostalCode);
					// attribute 'Orders.shipCountry'
					String firstNotNull_shippedOrder_ShipCountry = Util.getStringValue(r.getAs("shippedOrder_0.shipCountry"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String shippedOrder_shipCountry2 = Util.getStringValue(r.getAs("shippedOrder_" + i + ".shipCountry"));
						if (firstNotNull_shippedOrder_ShipCountry != null && shippedOrder_shipCountry2 != null && !firstNotNull_shippedOrder_ShipCountry.equals(shippedOrder_shipCountry2)) {
							ships_res.addLogEvent("Data consistency problem for [Orders - id :"+shippedOrder_res.getId()+"]: different values found for attribute 'Orders.shipCountry': " + firstNotNull_shippedOrder_ShipCountry + " and " + shippedOrder_shipCountry2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+shippedOrder_res.getId()+"]: different values found for attribute 'Orders.shipCountry': " + firstNotNull_shippedOrder_ShipCountry + " and " + shippedOrder_shipCountry2 + "." );
						}
						if (firstNotNull_shippedOrder_ShipCountry == null && shippedOrder_shipCountry2 != null) {
							firstNotNull_shippedOrder_ShipCountry = shippedOrder_shipCountry2;
						}
					}
					shippedOrder_res.setShipCountry(firstNotNull_shippedOrder_ShipCountry);
					// attribute 'Shippers.shipperID'
					Integer firstNotNull_shipper_shipperID = Util.getIntegerValue(r.getAs("shipper_0.shipperID"));
					shipper_res.setShipperID(firstNotNull_shipper_shipperID);
					// attribute 'Shippers.companyName'
					String firstNotNull_shipper_CompanyName = Util.getStringValue(r.getAs("shipper_0.companyName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String shipper_companyName2 = Util.getStringValue(r.getAs("shipper_" + i + ".companyName"));
						if (firstNotNull_shipper_CompanyName != null && shipper_companyName2 != null && !firstNotNull_shipper_CompanyName.equals(shipper_companyName2)) {
							ships_res.addLogEvent("Data consistency problem for [Shippers - id :"+shipper_res.getShipperID()+"]: different values found for attribute 'Shippers.companyName': " + firstNotNull_shipper_CompanyName + " and " + shipper_companyName2 + "." );
							logger.warn("Data consistency problem for [Shippers - id :"+shipper_res.getShipperID()+"]: different values found for attribute 'Shippers.companyName': " + firstNotNull_shipper_CompanyName + " and " + shipper_companyName2 + "." );
						}
						if (firstNotNull_shipper_CompanyName == null && shipper_companyName2 != null) {
							firstNotNull_shipper_CompanyName = shipper_companyName2;
						}
					}
					shipper_res.setCompanyName(firstNotNull_shipper_CompanyName);
					// attribute 'Shippers.phone'
					String firstNotNull_shipper_Phone = Util.getStringValue(r.getAs("shipper_0.phone"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String shipper_phone2 = Util.getStringValue(r.getAs("shipper_" + i + ".phone"));
						if (firstNotNull_shipper_Phone != null && shipper_phone2 != null && !firstNotNull_shipper_Phone.equals(shipper_phone2)) {
							ships_res.addLogEvent("Data consistency problem for [Shippers - id :"+shipper_res.getShipperID()+"]: different values found for attribute 'Shippers.phone': " + firstNotNull_shipper_Phone + " and " + shipper_phone2 + "." );
							logger.warn("Data consistency problem for [Shippers - id :"+shipper_res.getShipperID()+"]: different values found for attribute 'Shippers.phone': " + firstNotNull_shipper_Phone + " and " + shipper_phone2 + "." );
						}
						if (firstNotNull_shipper_Phone == null && shipper_phone2 != null) {
							firstNotNull_shipper_Phone = shipper_phone2;
						}
					}
					shipper_res.setPhone(firstNotNull_shipper_Phone);
	
					ships_res.setShippedOrder(shippedOrder_res);
					ships_res.setShipper(shipper_res);
					return ships_res;
		}
		, Encoders.bean(Ships.class));
	
	}
	
	//Empty arguments
	public Dataset<Ships> getShipsList(){
		 return getShipsList(null,null);
	}
	
	public abstract Dataset<Ships> getShipsList(
		Condition<OrdersAttribute> shippedOrder_condition,
		Condition<ShippersAttribute> shipper_condition);
	
	public Dataset<Ships> getShipsListByShippedOrderCondition(
		Condition<OrdersAttribute> shippedOrder_condition
	){
		return getShipsList(shippedOrder_condition, null);
	}
	
	public Ships getShipsByShippedOrder(Orders shippedOrder) {
		Condition<OrdersAttribute> cond = null;
		cond = Condition.simple(OrdersAttribute.id, Operator.EQUALS, shippedOrder.getId());
		Dataset<Ships> res = getShipsListByShippedOrderCondition(cond);
		List<Ships> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	public Dataset<Ships> getShipsListByShipperCondition(
		Condition<ShippersAttribute> shipper_condition
	){
		return getShipsList(null, shipper_condition);
	}
	
	public Dataset<Ships> getShipsListByShipper(Shippers shipper) {
		Condition<ShippersAttribute> cond = null;
		cond = Condition.simple(ShippersAttribute.shipperID, Operator.EQUALS, shipper.getShipperID());
		Dataset<Ships> res = getShipsListByShipperCondition(cond);
	return res;
	}
	
	
	
	public abstract void deleteShipsList(
		conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition,
		conditions.Condition<conditions.ShippersAttribute> shipper_condition);
	
	public void deleteShipsListByShippedOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition
	){
		deleteShipsList(shippedOrder_condition, null);
	}
	
	public void deleteShipsByShippedOrder(pojo.Orders shippedOrder) {
		// TODO using id for selecting
		return;
	}
	public void deleteShipsListByShipperCondition(
		conditions.Condition<conditions.ShippersAttribute> shipper_condition
	){
		deleteShipsList(null, shipper_condition);
	}
	
	public void deleteShipsListByShipper(pojo.Shippers shipper) {
		// TODO using id for selecting
		return;
	}
		
}
