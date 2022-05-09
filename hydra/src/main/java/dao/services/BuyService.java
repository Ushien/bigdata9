package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import conditions.*;
import pojo.Buy;
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


public abstract class BuyService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BuyService.class);
	
	
	// Left side 'CustomerRef' of reference [bought ]
	public abstract Dataset<OrdersTDO> getOrdersTDOListBoughtOrderInBoughtInOrdersFromMongoDB(Condition<OrdersAttribute> condition, MutableBoolean refilterFlag);
	
	// Right side 'custid' of reference [bought ]
	public abstract Dataset<CustomersTDO> getCustomersTDOListCustomerInBoughtInOrdersFromMongoDB(Condition<CustomersAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	// Left side 'orderref' of reference [purchases ]
	public abstract Dataset<CustomersTDO> getCustomersTDOListCustomerInPurchasesInCustomersPurchasedFromKvDB(Condition<CustomersAttribute> condition, MutableBoolean refilterFlag);
	
	// Right side 'OrderID' of reference [purchases ]
	public abstract Dataset<OrdersTDO> getOrdersTDOListBoughtOrderInPurchasesInCustomersPurchasedFromKvDB(Condition<OrdersAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	public static Dataset<Buy> fullLeftOuterJoinBetweenBuyAndBoughtOrder(Dataset<Buy> d1, Dataset<Orders> d2) {
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
		joinCond = d1.col("boughtOrder.id").equalTo(d2_.col("A_id"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Buy>) r -> {
				Buy res = new Buy();
	
				Orders boughtOrder = new Orders();
				Object o = r.getAs("boughtOrder");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						boughtOrder.setId(Util.getIntegerValue(r2.getAs("id")));
						boughtOrder.setOrderDate(Util.getLocalDateValue(r2.getAs("orderDate")));
						boughtOrder.setRequiredDate(Util.getLocalDateValue(r2.getAs("requiredDate")));
						boughtOrder.setShippedDate(Util.getLocalDateValue(r2.getAs("shippedDate")));
						boughtOrder.setFreight(Util.getDoubleValue(r2.getAs("freight")));
						boughtOrder.setShipName(Util.getStringValue(r2.getAs("shipName")));
						boughtOrder.setShipAddress(Util.getStringValue(r2.getAs("shipAddress")));
						boughtOrder.setShipCity(Util.getStringValue(r2.getAs("shipCity")));
						boughtOrder.setShipRegion(Util.getStringValue(r2.getAs("shipRegion")));
						boughtOrder.setShipPostalCode(Util.getStringValue(r2.getAs("shipPostalCode")));
						boughtOrder.setShipCountry(Util.getStringValue(r2.getAs("shipCountry")));
					} 
					if(o instanceof Orders) {
						boughtOrder = (Orders) o;
					}
				}
	
				res.setBoughtOrder(boughtOrder);
	
				Integer id = Util.getIntegerValue(r.getAs("A_id"));
				if (boughtOrder.getId() != null && id != null && !boughtOrder.getId().equals(id)) {
					res.addLogEvent("Data consistency problem for [Buy - different values found for attribute 'Buy.id': " + boughtOrder.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Buy - different values found for attribute 'Buy.id': " + boughtOrder.getId() + " and " + id + "." );
				}
				if(id != null)
					boughtOrder.setId(id);
				LocalDate orderDate = Util.getLocalDateValue(r.getAs("A_orderDate"));
				if (boughtOrder.getOrderDate() != null && orderDate != null && !boughtOrder.getOrderDate().equals(orderDate)) {
					res.addLogEvent("Data consistency problem for [Buy - different values found for attribute 'Buy.orderDate': " + boughtOrder.getOrderDate() + " and " + orderDate + "." );
					logger.warn("Data consistency problem for [Buy - different values found for attribute 'Buy.orderDate': " + boughtOrder.getOrderDate() + " and " + orderDate + "." );
				}
				if(orderDate != null)
					boughtOrder.setOrderDate(orderDate);
				LocalDate requiredDate = Util.getLocalDateValue(r.getAs("A_requiredDate"));
				if (boughtOrder.getRequiredDate() != null && requiredDate != null && !boughtOrder.getRequiredDate().equals(requiredDate)) {
					res.addLogEvent("Data consistency problem for [Buy - different values found for attribute 'Buy.requiredDate': " + boughtOrder.getRequiredDate() + " and " + requiredDate + "." );
					logger.warn("Data consistency problem for [Buy - different values found for attribute 'Buy.requiredDate': " + boughtOrder.getRequiredDate() + " and " + requiredDate + "." );
				}
				if(requiredDate != null)
					boughtOrder.setRequiredDate(requiredDate);
				LocalDate shippedDate = Util.getLocalDateValue(r.getAs("A_shippedDate"));
				if (boughtOrder.getShippedDate() != null && shippedDate != null && !boughtOrder.getShippedDate().equals(shippedDate)) {
					res.addLogEvent("Data consistency problem for [Buy - different values found for attribute 'Buy.shippedDate': " + boughtOrder.getShippedDate() + " and " + shippedDate + "." );
					logger.warn("Data consistency problem for [Buy - different values found for attribute 'Buy.shippedDate': " + boughtOrder.getShippedDate() + " and " + shippedDate + "." );
				}
				if(shippedDate != null)
					boughtOrder.setShippedDate(shippedDate);
				Double freight = Util.getDoubleValue(r.getAs("A_freight"));
				if (boughtOrder.getFreight() != null && freight != null && !boughtOrder.getFreight().equals(freight)) {
					res.addLogEvent("Data consistency problem for [Buy - different values found for attribute 'Buy.freight': " + boughtOrder.getFreight() + " and " + freight + "." );
					logger.warn("Data consistency problem for [Buy - different values found for attribute 'Buy.freight': " + boughtOrder.getFreight() + " and " + freight + "." );
				}
				if(freight != null)
					boughtOrder.setFreight(freight);
				String shipName = Util.getStringValue(r.getAs("A_shipName"));
				if (boughtOrder.getShipName() != null && shipName != null && !boughtOrder.getShipName().equals(shipName)) {
					res.addLogEvent("Data consistency problem for [Buy - different values found for attribute 'Buy.shipName': " + boughtOrder.getShipName() + " and " + shipName + "." );
					logger.warn("Data consistency problem for [Buy - different values found for attribute 'Buy.shipName': " + boughtOrder.getShipName() + " and " + shipName + "." );
				}
				if(shipName != null)
					boughtOrder.setShipName(shipName);
				String shipAddress = Util.getStringValue(r.getAs("A_shipAddress"));
				if (boughtOrder.getShipAddress() != null && shipAddress != null && !boughtOrder.getShipAddress().equals(shipAddress)) {
					res.addLogEvent("Data consistency problem for [Buy - different values found for attribute 'Buy.shipAddress': " + boughtOrder.getShipAddress() + " and " + shipAddress + "." );
					logger.warn("Data consistency problem for [Buy - different values found for attribute 'Buy.shipAddress': " + boughtOrder.getShipAddress() + " and " + shipAddress + "." );
				}
				if(shipAddress != null)
					boughtOrder.setShipAddress(shipAddress);
				String shipCity = Util.getStringValue(r.getAs("A_shipCity"));
				if (boughtOrder.getShipCity() != null && shipCity != null && !boughtOrder.getShipCity().equals(shipCity)) {
					res.addLogEvent("Data consistency problem for [Buy - different values found for attribute 'Buy.shipCity': " + boughtOrder.getShipCity() + " and " + shipCity + "." );
					logger.warn("Data consistency problem for [Buy - different values found for attribute 'Buy.shipCity': " + boughtOrder.getShipCity() + " and " + shipCity + "." );
				}
				if(shipCity != null)
					boughtOrder.setShipCity(shipCity);
				String shipRegion = Util.getStringValue(r.getAs("A_shipRegion"));
				if (boughtOrder.getShipRegion() != null && shipRegion != null && !boughtOrder.getShipRegion().equals(shipRegion)) {
					res.addLogEvent("Data consistency problem for [Buy - different values found for attribute 'Buy.shipRegion': " + boughtOrder.getShipRegion() + " and " + shipRegion + "." );
					logger.warn("Data consistency problem for [Buy - different values found for attribute 'Buy.shipRegion': " + boughtOrder.getShipRegion() + " and " + shipRegion + "." );
				}
				if(shipRegion != null)
					boughtOrder.setShipRegion(shipRegion);
				String shipPostalCode = Util.getStringValue(r.getAs("A_shipPostalCode"));
				if (boughtOrder.getShipPostalCode() != null && shipPostalCode != null && !boughtOrder.getShipPostalCode().equals(shipPostalCode)) {
					res.addLogEvent("Data consistency problem for [Buy - different values found for attribute 'Buy.shipPostalCode': " + boughtOrder.getShipPostalCode() + " and " + shipPostalCode + "." );
					logger.warn("Data consistency problem for [Buy - different values found for attribute 'Buy.shipPostalCode': " + boughtOrder.getShipPostalCode() + " and " + shipPostalCode + "." );
				}
				if(shipPostalCode != null)
					boughtOrder.setShipPostalCode(shipPostalCode);
				String shipCountry = Util.getStringValue(r.getAs("A_shipCountry"));
				if (boughtOrder.getShipCountry() != null && shipCountry != null && !boughtOrder.getShipCountry().equals(shipCountry)) {
					res.addLogEvent("Data consistency problem for [Buy - different values found for attribute 'Buy.shipCountry': " + boughtOrder.getShipCountry() + " and " + shipCountry + "." );
					logger.warn("Data consistency problem for [Buy - different values found for attribute 'Buy.shipCountry': " + boughtOrder.getShipCountry() + " and " + shipCountry + "." );
				}
				if(shipCountry != null)
					boughtOrder.setShipCountry(shipCountry);
	
				o = r.getAs("customer");
				Customers customer = new Customers();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						customer.setCustomerID(Util.getStringValue(r2.getAs("customerID")));
						customer.setCompanyName(Util.getStringValue(r2.getAs("companyName")));
						customer.setContactName(Util.getStringValue(r2.getAs("contactName")));
						customer.setContactTitle(Util.getStringValue(r2.getAs("contactTitle")));
						customer.setAddress(Util.getStringValue(r2.getAs("address")));
						customer.setCity(Util.getStringValue(r2.getAs("city")));
						customer.setRegion(Util.getStringValue(r2.getAs("region")));
						customer.setPostalCode(Util.getStringValue(r2.getAs("postalCode")));
						customer.setCountry(Util.getStringValue(r2.getAs("country")));
						customer.setPhone(Util.getStringValue(r2.getAs("phone")));
						customer.setFax(Util.getStringValue(r2.getAs("fax")));
					} 
					if(o instanceof Customers) {
						customer = (Customers) o;
					}
				}
	
				res.setCustomer(customer);
	
				return res;
		}, Encoders.bean(Buy.class));
	
		
		
	}
	public static Dataset<Buy> fullLeftOuterJoinBetweenBuyAndCustomer(Dataset<Buy> d1, Dataset<Customers> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("customerID", "A_customerID")
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
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("customer.customerID").equalTo(d2_.col("A_customerID"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Buy>) r -> {
				Buy res = new Buy();
	
				Customers customer = new Customers();
				Object o = r.getAs("customer");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						customer.setCustomerID(Util.getStringValue(r2.getAs("customerID")));
						customer.setCompanyName(Util.getStringValue(r2.getAs("companyName")));
						customer.setContactName(Util.getStringValue(r2.getAs("contactName")));
						customer.setContactTitle(Util.getStringValue(r2.getAs("contactTitle")));
						customer.setAddress(Util.getStringValue(r2.getAs("address")));
						customer.setCity(Util.getStringValue(r2.getAs("city")));
						customer.setRegion(Util.getStringValue(r2.getAs("region")));
						customer.setPostalCode(Util.getStringValue(r2.getAs("postalCode")));
						customer.setCountry(Util.getStringValue(r2.getAs("country")));
						customer.setPhone(Util.getStringValue(r2.getAs("phone")));
						customer.setFax(Util.getStringValue(r2.getAs("fax")));
					} 
					if(o instanceof Customers) {
						customer = (Customers) o;
					}
				}
	
				res.setCustomer(customer);
	
				String customerID = Util.getStringValue(r.getAs("A_customerID"));
				if (customer.getCustomerID() != null && customerID != null && !customer.getCustomerID().equals(customerID)) {
					res.addLogEvent("Data consistency problem for [Buy - different values found for attribute 'Buy.customerID': " + customer.getCustomerID() + " and " + customerID + "." );
					logger.warn("Data consistency problem for [Buy - different values found for attribute 'Buy.customerID': " + customer.getCustomerID() + " and " + customerID + "." );
				}
				if(customerID != null)
					customer.setCustomerID(customerID);
				String companyName = Util.getStringValue(r.getAs("A_companyName"));
				if (customer.getCompanyName() != null && companyName != null && !customer.getCompanyName().equals(companyName)) {
					res.addLogEvent("Data consistency problem for [Buy - different values found for attribute 'Buy.companyName': " + customer.getCompanyName() + " and " + companyName + "." );
					logger.warn("Data consistency problem for [Buy - different values found for attribute 'Buy.companyName': " + customer.getCompanyName() + " and " + companyName + "." );
				}
				if(companyName != null)
					customer.setCompanyName(companyName);
				String contactName = Util.getStringValue(r.getAs("A_contactName"));
				if (customer.getContactName() != null && contactName != null && !customer.getContactName().equals(contactName)) {
					res.addLogEvent("Data consistency problem for [Buy - different values found for attribute 'Buy.contactName': " + customer.getContactName() + " and " + contactName + "." );
					logger.warn("Data consistency problem for [Buy - different values found for attribute 'Buy.contactName': " + customer.getContactName() + " and " + contactName + "." );
				}
				if(contactName != null)
					customer.setContactName(contactName);
				String contactTitle = Util.getStringValue(r.getAs("A_contactTitle"));
				if (customer.getContactTitle() != null && contactTitle != null && !customer.getContactTitle().equals(contactTitle)) {
					res.addLogEvent("Data consistency problem for [Buy - different values found for attribute 'Buy.contactTitle': " + customer.getContactTitle() + " and " + contactTitle + "." );
					logger.warn("Data consistency problem for [Buy - different values found for attribute 'Buy.contactTitle': " + customer.getContactTitle() + " and " + contactTitle + "." );
				}
				if(contactTitle != null)
					customer.setContactTitle(contactTitle);
				String address = Util.getStringValue(r.getAs("A_address"));
				if (customer.getAddress() != null && address != null && !customer.getAddress().equals(address)) {
					res.addLogEvent("Data consistency problem for [Buy - different values found for attribute 'Buy.address': " + customer.getAddress() + " and " + address + "." );
					logger.warn("Data consistency problem for [Buy - different values found for attribute 'Buy.address': " + customer.getAddress() + " and " + address + "." );
				}
				if(address != null)
					customer.setAddress(address);
				String city = Util.getStringValue(r.getAs("A_city"));
				if (customer.getCity() != null && city != null && !customer.getCity().equals(city)) {
					res.addLogEvent("Data consistency problem for [Buy - different values found for attribute 'Buy.city': " + customer.getCity() + " and " + city + "." );
					logger.warn("Data consistency problem for [Buy - different values found for attribute 'Buy.city': " + customer.getCity() + " and " + city + "." );
				}
				if(city != null)
					customer.setCity(city);
				String region = Util.getStringValue(r.getAs("A_region"));
				if (customer.getRegion() != null && region != null && !customer.getRegion().equals(region)) {
					res.addLogEvent("Data consistency problem for [Buy - different values found for attribute 'Buy.region': " + customer.getRegion() + " and " + region + "." );
					logger.warn("Data consistency problem for [Buy - different values found for attribute 'Buy.region': " + customer.getRegion() + " and " + region + "." );
				}
				if(region != null)
					customer.setRegion(region);
				String postalCode = Util.getStringValue(r.getAs("A_postalCode"));
				if (customer.getPostalCode() != null && postalCode != null && !customer.getPostalCode().equals(postalCode)) {
					res.addLogEvent("Data consistency problem for [Buy - different values found for attribute 'Buy.postalCode': " + customer.getPostalCode() + " and " + postalCode + "." );
					logger.warn("Data consistency problem for [Buy - different values found for attribute 'Buy.postalCode': " + customer.getPostalCode() + " and " + postalCode + "." );
				}
				if(postalCode != null)
					customer.setPostalCode(postalCode);
				String country = Util.getStringValue(r.getAs("A_country"));
				if (customer.getCountry() != null && country != null && !customer.getCountry().equals(country)) {
					res.addLogEvent("Data consistency problem for [Buy - different values found for attribute 'Buy.country': " + customer.getCountry() + " and " + country + "." );
					logger.warn("Data consistency problem for [Buy - different values found for attribute 'Buy.country': " + customer.getCountry() + " and " + country + "." );
				}
				if(country != null)
					customer.setCountry(country);
				String phone = Util.getStringValue(r.getAs("A_phone"));
				if (customer.getPhone() != null && phone != null && !customer.getPhone().equals(phone)) {
					res.addLogEvent("Data consistency problem for [Buy - different values found for attribute 'Buy.phone': " + customer.getPhone() + " and " + phone + "." );
					logger.warn("Data consistency problem for [Buy - different values found for attribute 'Buy.phone': " + customer.getPhone() + " and " + phone + "." );
				}
				if(phone != null)
					customer.setPhone(phone);
				String fax = Util.getStringValue(r.getAs("A_fax"));
				if (customer.getFax() != null && fax != null && !customer.getFax().equals(fax)) {
					res.addLogEvent("Data consistency problem for [Buy - different values found for attribute 'Buy.fax': " + customer.getFax() + " and " + fax + "." );
					logger.warn("Data consistency problem for [Buy - different values found for attribute 'Buy.fax': " + customer.getFax() + " and " + fax + "." );
				}
				if(fax != null)
					customer.setFax(fax);
	
				o = r.getAs("boughtOrder");
				Orders boughtOrder = new Orders();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						boughtOrder.setId(Util.getIntegerValue(r2.getAs("id")));
						boughtOrder.setOrderDate(Util.getLocalDateValue(r2.getAs("orderDate")));
						boughtOrder.setRequiredDate(Util.getLocalDateValue(r2.getAs("requiredDate")));
						boughtOrder.setShippedDate(Util.getLocalDateValue(r2.getAs("shippedDate")));
						boughtOrder.setFreight(Util.getDoubleValue(r2.getAs("freight")));
						boughtOrder.setShipName(Util.getStringValue(r2.getAs("shipName")));
						boughtOrder.setShipAddress(Util.getStringValue(r2.getAs("shipAddress")));
						boughtOrder.setShipCity(Util.getStringValue(r2.getAs("shipCity")));
						boughtOrder.setShipRegion(Util.getStringValue(r2.getAs("shipRegion")));
						boughtOrder.setShipPostalCode(Util.getStringValue(r2.getAs("shipPostalCode")));
						boughtOrder.setShipCountry(Util.getStringValue(r2.getAs("shipCountry")));
					} 
					if(o instanceof Orders) {
						boughtOrder = (Orders) o;
					}
				}
	
				res.setBoughtOrder(boughtOrder);
	
				return res;
		}, Encoders.bean(Buy.class));
	
		
		
	}
	
	public static Dataset<Buy> fullOuterJoinsBuy(List<Dataset<Buy>> datasetsPOJO) {
		return fullOuterJoinsBuy(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Buy> fullLeftOuterJoinsBuy(List<Dataset<Buy>> datasetsPOJO) {
		return fullOuterJoinsBuy(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Buy> fullOuterJoinsBuy(List<Dataset<Buy>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		List<String> idFields = new ArrayList<String>();
		idFields.add("boughtOrder.id");
	
		idFields.add("customer.customerID");
		scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
		
		List<Dataset<Row>> rows = new ArrayList<Dataset<Row>>();
		for(int i = 0; i < datasetsPOJO.size(); i++) {
			Dataset<Buy> d = datasetsPOJO.get(i);
			rows.add(d
				.withColumn("boughtOrder_id_" + i, d.col("boughtOrder.id"))
				.withColumn("customer_customerID_" + i, d.col("customer.customerID"))
				.withColumnRenamed("boughtOrder", "boughtOrder_" + i)
				.withColumnRenamed("customer", "customer_" + i)
				.withColumnRenamed("logEvents", "logEvents_" + i));
		}
		
		Column joinCond;
		joinCond = rows.get(0).col("boughtOrder_id_0").equalTo(rows.get(1).col("boughtOrder_id_1"));
		joinCond = joinCond.and(rows.get(0).col("customer_customerID_0").equalTo(rows.get(1).col("customer_customerID_1")));
		
		Dataset<Row> res = rows.get(0).join(rows.get(1), joinCond, joinMode);
		for(int i = 2; i < rows.size(); i++) {
			joinCond = rows.get(i - 1).col("boughtOrder_id_" + (i - 1)).equalTo(rows.get(i).col("boughtOrder_id_" + i));
			joinCond = joinCond.and(rows.get(i - 1).col("customer_customerID_" + (i - 1)).equalTo(rows.get(i).col("customer_customerID_" + i)));
			res = res.join(rows.get(i), joinCond, joinMode);
		}
	
		return res.map((MapFunction<Row, Buy>) r -> {
				Buy buy_res = new Buy();
	
					WrappedArray logEvents = r.getAs("logEvents_0");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							buy_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							buy_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					Orders boughtOrder_res = new Orders();
					Customers customer_res = new Customers();
					
					// attribute 'Orders.id'
					Integer firstNotNull_boughtOrder_id = Util.getIntegerValue(r.getAs("boughtOrder_0.id"));
					boughtOrder_res.setId(firstNotNull_boughtOrder_id);
					// attribute 'Orders.orderDate'
					LocalDate firstNotNull_boughtOrder_orderDate = Util.getLocalDateValue(r.getAs("boughtOrder_0.orderDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate boughtOrder_orderDate2 = Util.getLocalDateValue(r.getAs("boughtOrder_" + i + ".orderDate"));
						if (firstNotNull_boughtOrder_orderDate != null && boughtOrder_orderDate2 != null && !firstNotNull_boughtOrder_orderDate.equals(boughtOrder_orderDate2)) {
							buy_res.addLogEvent("Data consistency problem for [Orders - id :"+boughtOrder_res.getId()+"]: different values found for attribute 'Orders.orderDate': " + firstNotNull_boughtOrder_orderDate + " and " + boughtOrder_orderDate2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+boughtOrder_res.getId()+"]: different values found for attribute 'Orders.orderDate': " + firstNotNull_boughtOrder_orderDate + " and " + boughtOrder_orderDate2 + "." );
						}
						if (firstNotNull_boughtOrder_orderDate == null && boughtOrder_orderDate2 != null) {
							firstNotNull_boughtOrder_orderDate = boughtOrder_orderDate2;
						}
					}
					boughtOrder_res.setOrderDate(firstNotNull_boughtOrder_orderDate);
					// attribute 'Orders.requiredDate'
					LocalDate firstNotNull_boughtOrder_requiredDate = Util.getLocalDateValue(r.getAs("boughtOrder_0.requiredDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate boughtOrder_requiredDate2 = Util.getLocalDateValue(r.getAs("boughtOrder_" + i + ".requiredDate"));
						if (firstNotNull_boughtOrder_requiredDate != null && boughtOrder_requiredDate2 != null && !firstNotNull_boughtOrder_requiredDate.equals(boughtOrder_requiredDate2)) {
							buy_res.addLogEvent("Data consistency problem for [Orders - id :"+boughtOrder_res.getId()+"]: different values found for attribute 'Orders.requiredDate': " + firstNotNull_boughtOrder_requiredDate + " and " + boughtOrder_requiredDate2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+boughtOrder_res.getId()+"]: different values found for attribute 'Orders.requiredDate': " + firstNotNull_boughtOrder_requiredDate + " and " + boughtOrder_requiredDate2 + "." );
						}
						if (firstNotNull_boughtOrder_requiredDate == null && boughtOrder_requiredDate2 != null) {
							firstNotNull_boughtOrder_requiredDate = boughtOrder_requiredDate2;
						}
					}
					boughtOrder_res.setRequiredDate(firstNotNull_boughtOrder_requiredDate);
					// attribute 'Orders.shippedDate'
					LocalDate firstNotNull_boughtOrder_shippedDate = Util.getLocalDateValue(r.getAs("boughtOrder_0.shippedDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate boughtOrder_shippedDate2 = Util.getLocalDateValue(r.getAs("boughtOrder_" + i + ".shippedDate"));
						if (firstNotNull_boughtOrder_shippedDate != null && boughtOrder_shippedDate2 != null && !firstNotNull_boughtOrder_shippedDate.equals(boughtOrder_shippedDate2)) {
							buy_res.addLogEvent("Data consistency problem for [Orders - id :"+boughtOrder_res.getId()+"]: different values found for attribute 'Orders.shippedDate': " + firstNotNull_boughtOrder_shippedDate + " and " + boughtOrder_shippedDate2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+boughtOrder_res.getId()+"]: different values found for attribute 'Orders.shippedDate': " + firstNotNull_boughtOrder_shippedDate + " and " + boughtOrder_shippedDate2 + "." );
						}
						if (firstNotNull_boughtOrder_shippedDate == null && boughtOrder_shippedDate2 != null) {
							firstNotNull_boughtOrder_shippedDate = boughtOrder_shippedDate2;
						}
					}
					boughtOrder_res.setShippedDate(firstNotNull_boughtOrder_shippedDate);
					// attribute 'Orders.freight'
					Double firstNotNull_boughtOrder_freight = Util.getDoubleValue(r.getAs("boughtOrder_0.freight"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double boughtOrder_freight2 = Util.getDoubleValue(r.getAs("boughtOrder_" + i + ".freight"));
						if (firstNotNull_boughtOrder_freight != null && boughtOrder_freight2 != null && !firstNotNull_boughtOrder_freight.equals(boughtOrder_freight2)) {
							buy_res.addLogEvent("Data consistency problem for [Orders - id :"+boughtOrder_res.getId()+"]: different values found for attribute 'Orders.freight': " + firstNotNull_boughtOrder_freight + " and " + boughtOrder_freight2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+boughtOrder_res.getId()+"]: different values found for attribute 'Orders.freight': " + firstNotNull_boughtOrder_freight + " and " + boughtOrder_freight2 + "." );
						}
						if (firstNotNull_boughtOrder_freight == null && boughtOrder_freight2 != null) {
							firstNotNull_boughtOrder_freight = boughtOrder_freight2;
						}
					}
					boughtOrder_res.setFreight(firstNotNull_boughtOrder_freight);
					// attribute 'Orders.shipName'
					String firstNotNull_boughtOrder_shipName = Util.getStringValue(r.getAs("boughtOrder_0.shipName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String boughtOrder_shipName2 = Util.getStringValue(r.getAs("boughtOrder_" + i + ".shipName"));
						if (firstNotNull_boughtOrder_shipName != null && boughtOrder_shipName2 != null && !firstNotNull_boughtOrder_shipName.equals(boughtOrder_shipName2)) {
							buy_res.addLogEvent("Data consistency problem for [Orders - id :"+boughtOrder_res.getId()+"]: different values found for attribute 'Orders.shipName': " + firstNotNull_boughtOrder_shipName + " and " + boughtOrder_shipName2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+boughtOrder_res.getId()+"]: different values found for attribute 'Orders.shipName': " + firstNotNull_boughtOrder_shipName + " and " + boughtOrder_shipName2 + "." );
						}
						if (firstNotNull_boughtOrder_shipName == null && boughtOrder_shipName2 != null) {
							firstNotNull_boughtOrder_shipName = boughtOrder_shipName2;
						}
					}
					boughtOrder_res.setShipName(firstNotNull_boughtOrder_shipName);
					// attribute 'Orders.shipAddress'
					String firstNotNull_boughtOrder_shipAddress = Util.getStringValue(r.getAs("boughtOrder_0.shipAddress"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String boughtOrder_shipAddress2 = Util.getStringValue(r.getAs("boughtOrder_" + i + ".shipAddress"));
						if (firstNotNull_boughtOrder_shipAddress != null && boughtOrder_shipAddress2 != null && !firstNotNull_boughtOrder_shipAddress.equals(boughtOrder_shipAddress2)) {
							buy_res.addLogEvent("Data consistency problem for [Orders - id :"+boughtOrder_res.getId()+"]: different values found for attribute 'Orders.shipAddress': " + firstNotNull_boughtOrder_shipAddress + " and " + boughtOrder_shipAddress2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+boughtOrder_res.getId()+"]: different values found for attribute 'Orders.shipAddress': " + firstNotNull_boughtOrder_shipAddress + " and " + boughtOrder_shipAddress2 + "." );
						}
						if (firstNotNull_boughtOrder_shipAddress == null && boughtOrder_shipAddress2 != null) {
							firstNotNull_boughtOrder_shipAddress = boughtOrder_shipAddress2;
						}
					}
					boughtOrder_res.setShipAddress(firstNotNull_boughtOrder_shipAddress);
					// attribute 'Orders.shipCity'
					String firstNotNull_boughtOrder_shipCity = Util.getStringValue(r.getAs("boughtOrder_0.shipCity"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String boughtOrder_shipCity2 = Util.getStringValue(r.getAs("boughtOrder_" + i + ".shipCity"));
						if (firstNotNull_boughtOrder_shipCity != null && boughtOrder_shipCity2 != null && !firstNotNull_boughtOrder_shipCity.equals(boughtOrder_shipCity2)) {
							buy_res.addLogEvent("Data consistency problem for [Orders - id :"+boughtOrder_res.getId()+"]: different values found for attribute 'Orders.shipCity': " + firstNotNull_boughtOrder_shipCity + " and " + boughtOrder_shipCity2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+boughtOrder_res.getId()+"]: different values found for attribute 'Orders.shipCity': " + firstNotNull_boughtOrder_shipCity + " and " + boughtOrder_shipCity2 + "." );
						}
						if (firstNotNull_boughtOrder_shipCity == null && boughtOrder_shipCity2 != null) {
							firstNotNull_boughtOrder_shipCity = boughtOrder_shipCity2;
						}
					}
					boughtOrder_res.setShipCity(firstNotNull_boughtOrder_shipCity);
					// attribute 'Orders.shipRegion'
					String firstNotNull_boughtOrder_shipRegion = Util.getStringValue(r.getAs("boughtOrder_0.shipRegion"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String boughtOrder_shipRegion2 = Util.getStringValue(r.getAs("boughtOrder_" + i + ".shipRegion"));
						if (firstNotNull_boughtOrder_shipRegion != null && boughtOrder_shipRegion2 != null && !firstNotNull_boughtOrder_shipRegion.equals(boughtOrder_shipRegion2)) {
							buy_res.addLogEvent("Data consistency problem for [Orders - id :"+boughtOrder_res.getId()+"]: different values found for attribute 'Orders.shipRegion': " + firstNotNull_boughtOrder_shipRegion + " and " + boughtOrder_shipRegion2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+boughtOrder_res.getId()+"]: different values found for attribute 'Orders.shipRegion': " + firstNotNull_boughtOrder_shipRegion + " and " + boughtOrder_shipRegion2 + "." );
						}
						if (firstNotNull_boughtOrder_shipRegion == null && boughtOrder_shipRegion2 != null) {
							firstNotNull_boughtOrder_shipRegion = boughtOrder_shipRegion2;
						}
					}
					boughtOrder_res.setShipRegion(firstNotNull_boughtOrder_shipRegion);
					// attribute 'Orders.shipPostalCode'
					String firstNotNull_boughtOrder_shipPostalCode = Util.getStringValue(r.getAs("boughtOrder_0.shipPostalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String boughtOrder_shipPostalCode2 = Util.getStringValue(r.getAs("boughtOrder_" + i + ".shipPostalCode"));
						if (firstNotNull_boughtOrder_shipPostalCode != null && boughtOrder_shipPostalCode2 != null && !firstNotNull_boughtOrder_shipPostalCode.equals(boughtOrder_shipPostalCode2)) {
							buy_res.addLogEvent("Data consistency problem for [Orders - id :"+boughtOrder_res.getId()+"]: different values found for attribute 'Orders.shipPostalCode': " + firstNotNull_boughtOrder_shipPostalCode + " and " + boughtOrder_shipPostalCode2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+boughtOrder_res.getId()+"]: different values found for attribute 'Orders.shipPostalCode': " + firstNotNull_boughtOrder_shipPostalCode + " and " + boughtOrder_shipPostalCode2 + "." );
						}
						if (firstNotNull_boughtOrder_shipPostalCode == null && boughtOrder_shipPostalCode2 != null) {
							firstNotNull_boughtOrder_shipPostalCode = boughtOrder_shipPostalCode2;
						}
					}
					boughtOrder_res.setShipPostalCode(firstNotNull_boughtOrder_shipPostalCode);
					// attribute 'Orders.shipCountry'
					String firstNotNull_boughtOrder_shipCountry = Util.getStringValue(r.getAs("boughtOrder_0.shipCountry"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String boughtOrder_shipCountry2 = Util.getStringValue(r.getAs("boughtOrder_" + i + ".shipCountry"));
						if (firstNotNull_boughtOrder_shipCountry != null && boughtOrder_shipCountry2 != null && !firstNotNull_boughtOrder_shipCountry.equals(boughtOrder_shipCountry2)) {
							buy_res.addLogEvent("Data consistency problem for [Orders - id :"+boughtOrder_res.getId()+"]: different values found for attribute 'Orders.shipCountry': " + firstNotNull_boughtOrder_shipCountry + " and " + boughtOrder_shipCountry2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+boughtOrder_res.getId()+"]: different values found for attribute 'Orders.shipCountry': " + firstNotNull_boughtOrder_shipCountry + " and " + boughtOrder_shipCountry2 + "." );
						}
						if (firstNotNull_boughtOrder_shipCountry == null && boughtOrder_shipCountry2 != null) {
							firstNotNull_boughtOrder_shipCountry = boughtOrder_shipCountry2;
						}
					}
					boughtOrder_res.setShipCountry(firstNotNull_boughtOrder_shipCountry);
					// attribute 'Customers.customerID'
					String firstNotNull_customer_customerID = Util.getStringValue(r.getAs("customer_0.customerID"));
					customer_res.setCustomerID(firstNotNull_customer_customerID);
					// attribute 'Customers.companyName'
					String firstNotNull_customer_companyName = Util.getStringValue(r.getAs("customer_0.companyName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String customer_companyName2 = Util.getStringValue(r.getAs("customer_" + i + ".companyName"));
						if (firstNotNull_customer_companyName != null && customer_companyName2 != null && !firstNotNull_customer_companyName.equals(customer_companyName2)) {
							buy_res.addLogEvent("Data consistency problem for [Customers - id :"+customer_res.getCustomerID()+"]: different values found for attribute 'Customers.companyName': " + firstNotNull_customer_companyName + " and " + customer_companyName2 + "." );
							logger.warn("Data consistency problem for [Customers - id :"+customer_res.getCustomerID()+"]: different values found for attribute 'Customers.companyName': " + firstNotNull_customer_companyName + " and " + customer_companyName2 + "." );
						}
						if (firstNotNull_customer_companyName == null && customer_companyName2 != null) {
							firstNotNull_customer_companyName = customer_companyName2;
						}
					}
					customer_res.setCompanyName(firstNotNull_customer_companyName);
					// attribute 'Customers.contactName'
					String firstNotNull_customer_contactName = Util.getStringValue(r.getAs("customer_0.contactName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String customer_contactName2 = Util.getStringValue(r.getAs("customer_" + i + ".contactName"));
						if (firstNotNull_customer_contactName != null && customer_contactName2 != null && !firstNotNull_customer_contactName.equals(customer_contactName2)) {
							buy_res.addLogEvent("Data consistency problem for [Customers - id :"+customer_res.getCustomerID()+"]: different values found for attribute 'Customers.contactName': " + firstNotNull_customer_contactName + " and " + customer_contactName2 + "." );
							logger.warn("Data consistency problem for [Customers - id :"+customer_res.getCustomerID()+"]: different values found for attribute 'Customers.contactName': " + firstNotNull_customer_contactName + " and " + customer_contactName2 + "." );
						}
						if (firstNotNull_customer_contactName == null && customer_contactName2 != null) {
							firstNotNull_customer_contactName = customer_contactName2;
						}
					}
					customer_res.setContactName(firstNotNull_customer_contactName);
					// attribute 'Customers.contactTitle'
					String firstNotNull_customer_contactTitle = Util.getStringValue(r.getAs("customer_0.contactTitle"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String customer_contactTitle2 = Util.getStringValue(r.getAs("customer_" + i + ".contactTitle"));
						if (firstNotNull_customer_contactTitle != null && customer_contactTitle2 != null && !firstNotNull_customer_contactTitle.equals(customer_contactTitle2)) {
							buy_res.addLogEvent("Data consistency problem for [Customers - id :"+customer_res.getCustomerID()+"]: different values found for attribute 'Customers.contactTitle': " + firstNotNull_customer_contactTitle + " and " + customer_contactTitle2 + "." );
							logger.warn("Data consistency problem for [Customers - id :"+customer_res.getCustomerID()+"]: different values found for attribute 'Customers.contactTitle': " + firstNotNull_customer_contactTitle + " and " + customer_contactTitle2 + "." );
						}
						if (firstNotNull_customer_contactTitle == null && customer_contactTitle2 != null) {
							firstNotNull_customer_contactTitle = customer_contactTitle2;
						}
					}
					customer_res.setContactTitle(firstNotNull_customer_contactTitle);
					// attribute 'Customers.address'
					String firstNotNull_customer_address = Util.getStringValue(r.getAs("customer_0.address"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String customer_address2 = Util.getStringValue(r.getAs("customer_" + i + ".address"));
						if (firstNotNull_customer_address != null && customer_address2 != null && !firstNotNull_customer_address.equals(customer_address2)) {
							buy_res.addLogEvent("Data consistency problem for [Customers - id :"+customer_res.getCustomerID()+"]: different values found for attribute 'Customers.address': " + firstNotNull_customer_address + " and " + customer_address2 + "." );
							logger.warn("Data consistency problem for [Customers - id :"+customer_res.getCustomerID()+"]: different values found for attribute 'Customers.address': " + firstNotNull_customer_address + " and " + customer_address2 + "." );
						}
						if (firstNotNull_customer_address == null && customer_address2 != null) {
							firstNotNull_customer_address = customer_address2;
						}
					}
					customer_res.setAddress(firstNotNull_customer_address);
					// attribute 'Customers.city'
					String firstNotNull_customer_city = Util.getStringValue(r.getAs("customer_0.city"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String customer_city2 = Util.getStringValue(r.getAs("customer_" + i + ".city"));
						if (firstNotNull_customer_city != null && customer_city2 != null && !firstNotNull_customer_city.equals(customer_city2)) {
							buy_res.addLogEvent("Data consistency problem for [Customers - id :"+customer_res.getCustomerID()+"]: different values found for attribute 'Customers.city': " + firstNotNull_customer_city + " and " + customer_city2 + "." );
							logger.warn("Data consistency problem for [Customers - id :"+customer_res.getCustomerID()+"]: different values found for attribute 'Customers.city': " + firstNotNull_customer_city + " and " + customer_city2 + "." );
						}
						if (firstNotNull_customer_city == null && customer_city2 != null) {
							firstNotNull_customer_city = customer_city2;
						}
					}
					customer_res.setCity(firstNotNull_customer_city);
					// attribute 'Customers.region'
					String firstNotNull_customer_region = Util.getStringValue(r.getAs("customer_0.region"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String customer_region2 = Util.getStringValue(r.getAs("customer_" + i + ".region"));
						if (firstNotNull_customer_region != null && customer_region2 != null && !firstNotNull_customer_region.equals(customer_region2)) {
							buy_res.addLogEvent("Data consistency problem for [Customers - id :"+customer_res.getCustomerID()+"]: different values found for attribute 'Customers.region': " + firstNotNull_customer_region + " and " + customer_region2 + "." );
							logger.warn("Data consistency problem for [Customers - id :"+customer_res.getCustomerID()+"]: different values found for attribute 'Customers.region': " + firstNotNull_customer_region + " and " + customer_region2 + "." );
						}
						if (firstNotNull_customer_region == null && customer_region2 != null) {
							firstNotNull_customer_region = customer_region2;
						}
					}
					customer_res.setRegion(firstNotNull_customer_region);
					// attribute 'Customers.postalCode'
					String firstNotNull_customer_postalCode = Util.getStringValue(r.getAs("customer_0.postalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String customer_postalCode2 = Util.getStringValue(r.getAs("customer_" + i + ".postalCode"));
						if (firstNotNull_customer_postalCode != null && customer_postalCode2 != null && !firstNotNull_customer_postalCode.equals(customer_postalCode2)) {
							buy_res.addLogEvent("Data consistency problem for [Customers - id :"+customer_res.getCustomerID()+"]: different values found for attribute 'Customers.postalCode': " + firstNotNull_customer_postalCode + " and " + customer_postalCode2 + "." );
							logger.warn("Data consistency problem for [Customers - id :"+customer_res.getCustomerID()+"]: different values found for attribute 'Customers.postalCode': " + firstNotNull_customer_postalCode + " and " + customer_postalCode2 + "." );
						}
						if (firstNotNull_customer_postalCode == null && customer_postalCode2 != null) {
							firstNotNull_customer_postalCode = customer_postalCode2;
						}
					}
					customer_res.setPostalCode(firstNotNull_customer_postalCode);
					// attribute 'Customers.country'
					String firstNotNull_customer_country = Util.getStringValue(r.getAs("customer_0.country"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String customer_country2 = Util.getStringValue(r.getAs("customer_" + i + ".country"));
						if (firstNotNull_customer_country != null && customer_country2 != null && !firstNotNull_customer_country.equals(customer_country2)) {
							buy_res.addLogEvent("Data consistency problem for [Customers - id :"+customer_res.getCustomerID()+"]: different values found for attribute 'Customers.country': " + firstNotNull_customer_country + " and " + customer_country2 + "." );
							logger.warn("Data consistency problem for [Customers - id :"+customer_res.getCustomerID()+"]: different values found for attribute 'Customers.country': " + firstNotNull_customer_country + " and " + customer_country2 + "." );
						}
						if (firstNotNull_customer_country == null && customer_country2 != null) {
							firstNotNull_customer_country = customer_country2;
						}
					}
					customer_res.setCountry(firstNotNull_customer_country);
					// attribute 'Customers.phone'
					String firstNotNull_customer_phone = Util.getStringValue(r.getAs("customer_0.phone"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String customer_phone2 = Util.getStringValue(r.getAs("customer_" + i + ".phone"));
						if (firstNotNull_customer_phone != null && customer_phone2 != null && !firstNotNull_customer_phone.equals(customer_phone2)) {
							buy_res.addLogEvent("Data consistency problem for [Customers - id :"+customer_res.getCustomerID()+"]: different values found for attribute 'Customers.phone': " + firstNotNull_customer_phone + " and " + customer_phone2 + "." );
							logger.warn("Data consistency problem for [Customers - id :"+customer_res.getCustomerID()+"]: different values found for attribute 'Customers.phone': " + firstNotNull_customer_phone + " and " + customer_phone2 + "." );
						}
						if (firstNotNull_customer_phone == null && customer_phone2 != null) {
							firstNotNull_customer_phone = customer_phone2;
						}
					}
					customer_res.setPhone(firstNotNull_customer_phone);
					// attribute 'Customers.fax'
					String firstNotNull_customer_fax = Util.getStringValue(r.getAs("customer_0.fax"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String customer_fax2 = Util.getStringValue(r.getAs("customer_" + i + ".fax"));
						if (firstNotNull_customer_fax != null && customer_fax2 != null && !firstNotNull_customer_fax.equals(customer_fax2)) {
							buy_res.addLogEvent("Data consistency problem for [Customers - id :"+customer_res.getCustomerID()+"]: different values found for attribute 'Customers.fax': " + firstNotNull_customer_fax + " and " + customer_fax2 + "." );
							logger.warn("Data consistency problem for [Customers - id :"+customer_res.getCustomerID()+"]: different values found for attribute 'Customers.fax': " + firstNotNull_customer_fax + " and " + customer_fax2 + "." );
						}
						if (firstNotNull_customer_fax == null && customer_fax2 != null) {
							firstNotNull_customer_fax = customer_fax2;
						}
					}
					customer_res.setFax(firstNotNull_customer_fax);
	
					buy_res.setBoughtOrder(boughtOrder_res);
					buy_res.setCustomer(customer_res);
					return buy_res;
		}
		, Encoders.bean(Buy.class));
	
	}
	
	//Empty arguments
	public Dataset<Buy> getBuyList(){
		 return getBuyList(null,null);
	}
	
	public abstract Dataset<Buy> getBuyList(
		Condition<OrdersAttribute> boughtOrder_condition,
		Condition<CustomersAttribute> customer_condition);
	
	public Dataset<Buy> getBuyListByBoughtOrderCondition(
		Condition<OrdersAttribute> boughtOrder_condition
	){
		return getBuyList(boughtOrder_condition, null);
	}
	
	public Buy getBuyByBoughtOrder(Orders boughtOrder) {
		Condition<OrdersAttribute> cond = null;
		cond = Condition.simple(OrdersAttribute.id, Operator.EQUALS, boughtOrder.getId());
		Dataset<Buy> res = getBuyListByBoughtOrderCondition(cond);
		List<Buy> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	public Dataset<Buy> getBuyListByCustomerCondition(
		Condition<CustomersAttribute> customer_condition
	){
		return getBuyList(null, customer_condition);
	}
	
	public Dataset<Buy> getBuyListByCustomer(Customers customer) {
		Condition<CustomersAttribute> cond = null;
		cond = Condition.simple(CustomersAttribute.customerID, Operator.EQUALS, customer.getCustomerID());
		Dataset<Buy> res = getBuyListByCustomerCondition(cond);
	return res;
	}
	
	
	
	public abstract void deleteBuyList(
		conditions.Condition<conditions.OrdersAttribute> boughtOrder_condition,
		conditions.Condition<conditions.CustomersAttribute> customer_condition);
	
	public void deleteBuyListByBoughtOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> boughtOrder_condition
	){
		deleteBuyList(boughtOrder_condition, null);
	}
	
	public void deleteBuyByBoughtOrder(pojo.Orders boughtOrder) {
		// TODO using id for selecting
		return;
	}
	public void deleteBuyListByCustomerCondition(
		conditions.Condition<conditions.CustomersAttribute> customer_condition
	){
		deleteBuyList(null, customer_condition);
	}
	
	public void deleteBuyListByCustomer(pojo.Customers customer) {
		// TODO using id for selecting
		return;
	}
		
}
