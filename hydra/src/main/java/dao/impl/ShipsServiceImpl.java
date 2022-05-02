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
import conditions.ShipsAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.OrdersTDO;
import tdo.ShipsTDO;
import conditions.OrdersAttribute;
import dao.services.OrdersService;
import tdo.ShippersTDO;
import tdo.ShipsTDO;
import conditions.ShippersAttribute;
import dao.services.ShippersService;
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

public class ShipsServiceImpl extends dao.services.ShipsService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ShipsServiceImpl.class);
	
	// method accessing the embedded object ShipmentInfo mapped to role shippedOrder
	public Dataset<Ships> getShipsListInmongoDBOrdersShipmentInfo(Condition<OrdersAttribute> shippedOrder_condition, Condition<ShippersAttribute> shipper_condition, MutableBoolean shippedOrder_refilter, MutableBoolean shipper_refilter){	
			List<String> bsons = new ArrayList<String>();
			String bson = null;
			bson = OrdersServiceImpl.getBSONMatchQueryInOrdersFromMyMongoDB(shippedOrder_condition ,shippedOrder_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
			bson = ShippersServiceImpl.getBSONMatchQueryInOrdersFromMyMongoDB(shipper_condition ,shipper_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
		
			String bsonQuery = bsons.size() == 0 ? null : "{$match: { $and: [" + String.join(",", bsons) + "] }}";
		
			Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Orders", bsonQuery);
		
			Dataset<Ships> res = dataset.flatMap((FlatMapFunction<Row, Ships>) r -> {
					List<Ships> list_res = new ArrayList<Ships>();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					Row nestedRow = null;
		
					boolean addedInList = false;
					Row r1 = r;
					Ships ships1 = new Ships();
					ships1.setShippedOrder(new Orders());
					ships1.setShipper(new Shippers());
					
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Orders.id for field OrderID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderID")) {
						if(nestedRow.getAs("OrderID")==null)
							ships1.getShippedOrder().setId(null);
						else{
							ships1.getShippedOrder().setId(Util.getIntegerValue(nestedRow.getAs("OrderID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.OrderDate for field OrderDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderDate")) {
						if(nestedRow.getAs("OrderDate")==null)
							ships1.getShippedOrder().setOrderDate(null);
						else{
							ships1.getShippedOrder().setOrderDate(Util.getLocalDateValue(nestedRow.getAs("OrderDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.RequiredDate for field RequiredDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("RequiredDate")) {
						if(nestedRow.getAs("RequiredDate")==null)
							ships1.getShippedOrder().setRequiredDate(null);
						else{
							ships1.getShippedOrder().setRequiredDate(Util.getLocalDateValue(nestedRow.getAs("RequiredDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.Freight for field Freight			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Freight")) {
						if(nestedRow.getAs("Freight")==null)
							ships1.getShippedOrder().setFreight(null);
						else{
							ships1.getShippedOrder().setFreight(Util.getDoubleValue(nestedRow.getAs("Freight")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.ShippedDate for field ShippedDate			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShippedDate")) {
						if(nestedRow.getAs("ShippedDate")==null)
							ships1.getShippedOrder().setShippedDate(null);
						else{
							ships1.getShippedOrder().setShippedDate(Util.getLocalDateValue(nestedRow.getAs("ShippedDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Shippers.shipperID for field ShipperID			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipperID")) {
						if(nestedRow.getAs("ShipperID")==null)
							ships1.getShipper().setShipperID(null);
						else{
							ships1.getShipper().setShipperID(Util.getIntegerValue(nestedRow.getAs("ShipperID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Shippers.CompanyName for field CompanyName			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("CompanyName")) {
						if(nestedRow.getAs("CompanyName")==null)
							ships1.getShipper().setCompanyName(null);
						else{
							ships1.getShipper().setCompanyName(Util.getStringValue(nestedRow.getAs("CompanyName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Shippers.Phone for field Phone			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Phone")) {
						if(nestedRow.getAs("Phone")==null)
							ships1.getShipper().setPhone(null);
						else{
							ships1.getShipper().setPhone(Util.getStringValue(nestedRow.getAs("Phone")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.ShipName for field ShipName			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipName")) {
						if(nestedRow.getAs("ShipName")==null)
							ships1.getShippedOrder().setShipName(null);
						else{
							ships1.getShippedOrder().setShipName(Util.getStringValue(nestedRow.getAs("ShipName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.ShipAddress for field ShipAddress			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipAddress")) {
						if(nestedRow.getAs("ShipAddress")==null)
							ships1.getShippedOrder().setShipAddress(null);
						else{
							ships1.getShippedOrder().setShipAddress(Util.getStringValue(nestedRow.getAs("ShipAddress")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.ShipCity for field ShipCity			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCity")) {
						if(nestedRow.getAs("ShipCity")==null)
							ships1.getShippedOrder().setShipCity(null);
						else{
							ships1.getShippedOrder().setShipCity(Util.getStringValue(nestedRow.getAs("ShipCity")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.ShipRegion for field ShipRegion			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipRegion")) {
						if(nestedRow.getAs("ShipRegion")==null)
							ships1.getShippedOrder().setShipRegion(null);
						else{
							ships1.getShippedOrder().setShipRegion(Util.getStringValue(nestedRow.getAs("ShipRegion")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.ShipPostalCode for field ShipPostalCode			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipPostalCode")) {
						if(nestedRow.getAs("ShipPostalCode")==null)
							ships1.getShippedOrder().setShipPostalCode(null);
						else{
							ships1.getShippedOrder().setShipPostalCode(Util.getStringValue(nestedRow.getAs("ShipPostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.ShipCountry for field ShipCountry			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCountry")) {
						if(nestedRow.getAs("ShipCountry")==null)
							ships1.getShippedOrder().setShipCountry(null);
						else{
							ships1.getShippedOrder().setShipCountry(Util.getStringValue(nestedRow.getAs("ShipCountry")));
							toAdd1 = true;					
							}
					}
					if(toAdd1 ) {
						if(!(ships1.getShippedOrder().equals(new Orders())) && !(ships1.getShipper().equals(new Shippers())))
							list_res.add(ships1);
						addedInList = true;
					} 
					
					
					return list_res.iterator();
		
			}, Encoders.bean(Ships.class));
			// TODO drop duplicates based on roles ids
			//res= res.dropDuplicates(new String {});
			return res;
	}
	
	
	public Dataset<Ships> getShipsList(
		Condition<OrdersAttribute> shippedOrder_condition,
		Condition<ShippersAttribute> shipper_condition){
			ShipsServiceImpl shipsService = this;
			OrdersService ordersService = new OrdersServiceImpl();  
			ShippersService shippersService = new ShippersServiceImpl();
			MutableBoolean shippedOrder_refilter = new MutableBoolean(false);
			List<Dataset<Ships>> datasetsPOJO = new ArrayList<Dataset<Ships>>();
			boolean all_already_persisted = false;
			MutableBoolean shipper_refilter = new MutableBoolean(false);
			
			org.apache.spark.sql.Column joinCondition = null;
		
			
			Dataset<Ships> res_ships_shippedOrder;
			Dataset<Orders> res_Orders;
			// Role 'shippedOrder' mapped to EmbeddedObject 'ShipmentInfo' - 'Shippers' containing 'Orders'
			shipper_refilter = new MutableBoolean(false);
			res_ships_shippedOrder = shipsService.getShipsListInmongoDBOrdersShipmentInfo(shippedOrder_condition, shipper_condition, shippedOrder_refilter, shipper_refilter);
		 	
			datasetsPOJO.add(res_ships_shippedOrder);
			
			
			//Join datasets or return 
			Dataset<Ships> res = fullOuterJoinsShips(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Orders> lonelyShippedOrder = null;
			Dataset<Shippers> lonelyShipper = null;
			
		
		
			
			if(shippedOrder_refilter.booleanValue() || shipper_refilter.booleanValue())
				res = res.filter((FilterFunction<Ships>) r -> (shippedOrder_condition == null || shippedOrder_condition.evaluate(r.getShippedOrder())) && (shipper_condition == null || shipper_condition.evaluate(r.getShipper())));
			
		
			return res;
		
		}
	
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
	
	
	
	public void deleteShipsList(
		conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition,
		conditions.Condition<conditions.ShippersAttribute> shipper_condition){
			//TODO
		}
	
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
