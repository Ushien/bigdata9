package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import pojo.Orders;
import conditions.OrdersAttribute;
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
import conditions.OrdersAttribute;
import pojo.Buy;
import conditions.CustomersAttribute;
import pojo.Customers;
import conditions.OrdersAttribute;
import pojo.Register;
import conditions.EmployeesAttribute;
import pojo.Employees;
import conditions.OrdersAttribute;
import pojo.Ships;
import conditions.ShippersAttribute;
import pojo.Shippers;
import conditions.OrdersAttribute;
import pojo.ComposedOf;
import conditions.ProductsAttribute;
import pojo.Products;

public abstract class OrdersService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OrdersService.class);
	protected BuyService buyService = new dao.impl.BuyServiceImpl();
	protected RegisterService registerService = new dao.impl.RegisterServiceImpl();
	protected ShipsService shipsService = new dao.impl.ShipsServiceImpl();
	protected ComposedOfService composedOfService = new dao.impl.ComposedOfServiceImpl();
	


	public static enum ROLE_NAME {
		BUY_BOUGHTORDER, REGISTER_PROCESSEDORDER, SHIPS_SHIPPEDORDER, COMPOSEDOF_ORDER
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.BUY_BOUGHTORDER, loading.Loading.EAGER);
		defaultLoadingParameters.put(ROLE_NAME.REGISTER_PROCESSEDORDER, loading.Loading.EAGER);
		defaultLoadingParameters.put(ROLE_NAME.SHIPS_SHIPPEDORDER, loading.Loading.EAGER);
		defaultLoadingParameters.put(ROLE_NAME.COMPOSEDOF_ORDER, loading.Loading.LAZY);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public OrdersService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public OrdersService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
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
	
	
	public Dataset<Orders> getOrdersList(){
		return getOrdersList(null);
	}
	
	public Dataset<Orders> getOrdersList(conditions.Condition<conditions.OrdersAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Orders>> datasets = new ArrayList<Dataset<Orders>>();
		Dataset<Orders> d = null;
		d = getOrdersListInOrdersFromMyMongoDB(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsOrders(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Orders>) r -> condition == null || condition.evaluate(r));
		d = d.dropDuplicates(new String[] {"id"});
		return d;
	}
	
	
	
	
	
	public abstract Dataset<Orders> getOrdersListInOrdersFromMyMongoDB(conditions.Condition<conditions.OrdersAttribute> condition, MutableBoolean refilterFlag);
	
	
	public Orders getOrdersById(Integer id){
		Condition cond;
		cond = Condition.simple(OrdersAttribute.id, conditions.Operator.EQUALS, id);
		Dataset<Orders> res = getOrdersList(cond);
		if(res!=null && !res.isEmpty())
			return res.first();
		return null;
	}
	
	public Dataset<Orders> getOrdersListById(Integer id) {
		return getOrdersList(conditions.Condition.simple(conditions.OrdersAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Orders> getOrdersListByOrderDate(LocalDate OrderDate) {
		return getOrdersList(conditions.Condition.simple(conditions.OrdersAttribute.OrderDate, conditions.Operator.EQUALS, OrderDate));
	}
	
	public Dataset<Orders> getOrdersListByRequiredDate(LocalDate RequiredDate) {
		return getOrdersList(conditions.Condition.simple(conditions.OrdersAttribute.RequiredDate, conditions.Operator.EQUALS, RequiredDate));
	}
	
	public Dataset<Orders> getOrdersListByShippedDate(LocalDate ShippedDate) {
		return getOrdersList(conditions.Condition.simple(conditions.OrdersAttribute.ShippedDate, conditions.Operator.EQUALS, ShippedDate));
	}
	
	public Dataset<Orders> getOrdersListByFreight(Double Freight) {
		return getOrdersList(conditions.Condition.simple(conditions.OrdersAttribute.Freight, conditions.Operator.EQUALS, Freight));
	}
	
	public Dataset<Orders> getOrdersListByShipName(String ShipName) {
		return getOrdersList(conditions.Condition.simple(conditions.OrdersAttribute.ShipName, conditions.Operator.EQUALS, ShipName));
	}
	
	public Dataset<Orders> getOrdersListByShipAddress(String ShipAddress) {
		return getOrdersList(conditions.Condition.simple(conditions.OrdersAttribute.ShipAddress, conditions.Operator.EQUALS, ShipAddress));
	}
	
	public Dataset<Orders> getOrdersListByShipCity(String ShipCity) {
		return getOrdersList(conditions.Condition.simple(conditions.OrdersAttribute.ShipCity, conditions.Operator.EQUALS, ShipCity));
	}
	
	public Dataset<Orders> getOrdersListByShipRegion(String ShipRegion) {
		return getOrdersList(conditions.Condition.simple(conditions.OrdersAttribute.ShipRegion, conditions.Operator.EQUALS, ShipRegion));
	}
	
	public Dataset<Orders> getOrdersListByShipPostalCode(String ShipPostalCode) {
		return getOrdersList(conditions.Condition.simple(conditions.OrdersAttribute.ShipPostalCode, conditions.Operator.EQUALS, ShipPostalCode));
	}
	
	public Dataset<Orders> getOrdersListByShipCountry(String ShipCountry) {
		return getOrdersList(conditions.Condition.simple(conditions.OrdersAttribute.ShipCountry, conditions.Operator.EQUALS, ShipCountry));
	}
	
	
	
	public static Dataset<Orders> fullOuterJoinsOrders(List<Dataset<Orders>> datasetsPOJO) {
		return fullOuterJoinsOrders(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Orders> fullLeftOuterJoinsOrders(List<Dataset<Orders>> datasetsPOJO) {
		return fullOuterJoinsOrders(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Orders> fullOuterJoinsOrders(List<Dataset<Orders>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Orders> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("id");
			logger.debug("Start {} of [{}] datasets of [Orders] objects",joinMode,datasetsPOJO.size());
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("orderDate", "orderDate_1")
								.withColumnRenamed("requiredDate", "requiredDate_1")
								.withColumnRenamed("shippedDate", "shippedDate_1")
								.withColumnRenamed("freight", "freight_1")
								.withColumnRenamed("shipName", "shipName_1")
								.withColumnRenamed("shipAddress", "shipAddress_1")
								.withColumnRenamed("shipCity", "shipCity_1")
								.withColumnRenamed("shipRegion", "shipRegion_1")
								.withColumnRenamed("shipPostalCode", "shipPostalCode_1")
								.withColumnRenamed("shipCountry", "shipCountry_1")
								.withColumnRenamed("logEvents", "logEvents_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("orderDate", "orderDate_" + i)
								.withColumnRenamed("requiredDate", "requiredDate_" + i)
								.withColumnRenamed("shippedDate", "shippedDate_" + i)
								.withColumnRenamed("freight", "freight_" + i)
								.withColumnRenamed("shipName", "shipName_" + i)
								.withColumnRenamed("shipAddress", "shipAddress_" + i)
								.withColumnRenamed("shipCity", "shipCity_" + i)
								.withColumnRenamed("shipRegion", "shipRegion_" + i)
								.withColumnRenamed("shipPostalCode", "shipPostalCode_" + i)
								.withColumnRenamed("shipCountry", "shipCountry_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
						, seq, joinMode);
			}
			logger.debug("End join. Start");
			logger.debug("Start transforming Row objects to [Orders] objects"); 
			d = res.map((MapFunction<Row, Orders>) r -> {
					Orders orders_res = new Orders();
					
					// attribute 'Orders.id'
					Integer firstNotNull_id = Util.getIntegerValue(r.getAs("id"));
					orders_res.setId(firstNotNull_id);
					
					// attribute 'Orders.orderDate'
					LocalDate firstNotNull_OrderDate = Util.getLocalDateValue(r.getAs("orderDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate orderDate2 = Util.getLocalDateValue(r.getAs("orderDate_" + i));
						if (firstNotNull_OrderDate != null && orderDate2 != null && !firstNotNull_OrderDate.equals(orderDate2)) {
							orders_res.addLogEvent("Data consistency problem for [Orders - id :"+orders_res.getId()+"]: different values found for attribute 'Orders.orderDate': " + firstNotNull_OrderDate + " and " + orderDate2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+orders_res.getId()+"]: different values found for attribute 'Orders.orderDate': " + firstNotNull_OrderDate + " and " + orderDate2 + "." );
						}
						if (firstNotNull_OrderDate == null && orderDate2 != null) {
							firstNotNull_OrderDate = orderDate2;
						}
					}
					orders_res.setOrderDate(firstNotNull_OrderDate);
					
					// attribute 'Orders.requiredDate'
					LocalDate firstNotNull_RequiredDate = Util.getLocalDateValue(r.getAs("requiredDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate requiredDate2 = Util.getLocalDateValue(r.getAs("requiredDate_" + i));
						if (firstNotNull_RequiredDate != null && requiredDate2 != null && !firstNotNull_RequiredDate.equals(requiredDate2)) {
							orders_res.addLogEvent("Data consistency problem for [Orders - id :"+orders_res.getId()+"]: different values found for attribute 'Orders.requiredDate': " + firstNotNull_RequiredDate + " and " + requiredDate2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+orders_res.getId()+"]: different values found for attribute 'Orders.requiredDate': " + firstNotNull_RequiredDate + " and " + requiredDate2 + "." );
						}
						if (firstNotNull_RequiredDate == null && requiredDate2 != null) {
							firstNotNull_RequiredDate = requiredDate2;
						}
					}
					orders_res.setRequiredDate(firstNotNull_RequiredDate);
					
					// attribute 'Orders.shippedDate'
					LocalDate firstNotNull_ShippedDate = Util.getLocalDateValue(r.getAs("shippedDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate shippedDate2 = Util.getLocalDateValue(r.getAs("shippedDate_" + i));
						if (firstNotNull_ShippedDate != null && shippedDate2 != null && !firstNotNull_ShippedDate.equals(shippedDate2)) {
							orders_res.addLogEvent("Data consistency problem for [Orders - id :"+orders_res.getId()+"]: different values found for attribute 'Orders.shippedDate': " + firstNotNull_ShippedDate + " and " + shippedDate2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+orders_res.getId()+"]: different values found for attribute 'Orders.shippedDate': " + firstNotNull_ShippedDate + " and " + shippedDate2 + "." );
						}
						if (firstNotNull_ShippedDate == null && shippedDate2 != null) {
							firstNotNull_ShippedDate = shippedDate2;
						}
					}
					orders_res.setShippedDate(firstNotNull_ShippedDate);
					
					// attribute 'Orders.freight'
					Double firstNotNull_Freight = Util.getDoubleValue(r.getAs("freight"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double freight2 = Util.getDoubleValue(r.getAs("freight_" + i));
						if (firstNotNull_Freight != null && freight2 != null && !firstNotNull_Freight.equals(freight2)) {
							orders_res.addLogEvent("Data consistency problem for [Orders - id :"+orders_res.getId()+"]: different values found for attribute 'Orders.freight': " + firstNotNull_Freight + " and " + freight2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+orders_res.getId()+"]: different values found for attribute 'Orders.freight': " + firstNotNull_Freight + " and " + freight2 + "." );
						}
						if (firstNotNull_Freight == null && freight2 != null) {
							firstNotNull_Freight = freight2;
						}
					}
					orders_res.setFreight(firstNotNull_Freight);
					
					// attribute 'Orders.shipName'
					String firstNotNull_ShipName = Util.getStringValue(r.getAs("shipName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String shipName2 = Util.getStringValue(r.getAs("shipName_" + i));
						if (firstNotNull_ShipName != null && shipName2 != null && !firstNotNull_ShipName.equals(shipName2)) {
							orders_res.addLogEvent("Data consistency problem for [Orders - id :"+orders_res.getId()+"]: different values found for attribute 'Orders.shipName': " + firstNotNull_ShipName + " and " + shipName2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+orders_res.getId()+"]: different values found for attribute 'Orders.shipName': " + firstNotNull_ShipName + " and " + shipName2 + "." );
						}
						if (firstNotNull_ShipName == null && shipName2 != null) {
							firstNotNull_ShipName = shipName2;
						}
					}
					orders_res.setShipName(firstNotNull_ShipName);
					
					// attribute 'Orders.shipAddress'
					String firstNotNull_ShipAddress = Util.getStringValue(r.getAs("shipAddress"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String shipAddress2 = Util.getStringValue(r.getAs("shipAddress_" + i));
						if (firstNotNull_ShipAddress != null && shipAddress2 != null && !firstNotNull_ShipAddress.equals(shipAddress2)) {
							orders_res.addLogEvent("Data consistency problem for [Orders - id :"+orders_res.getId()+"]: different values found for attribute 'Orders.shipAddress': " + firstNotNull_ShipAddress + " and " + shipAddress2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+orders_res.getId()+"]: different values found for attribute 'Orders.shipAddress': " + firstNotNull_ShipAddress + " and " + shipAddress2 + "." );
						}
						if (firstNotNull_ShipAddress == null && shipAddress2 != null) {
							firstNotNull_ShipAddress = shipAddress2;
						}
					}
					orders_res.setShipAddress(firstNotNull_ShipAddress);
					
					// attribute 'Orders.shipCity'
					String firstNotNull_ShipCity = Util.getStringValue(r.getAs("shipCity"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String shipCity2 = Util.getStringValue(r.getAs("shipCity_" + i));
						if (firstNotNull_ShipCity != null && shipCity2 != null && !firstNotNull_ShipCity.equals(shipCity2)) {
							orders_res.addLogEvent("Data consistency problem for [Orders - id :"+orders_res.getId()+"]: different values found for attribute 'Orders.shipCity': " + firstNotNull_ShipCity + " and " + shipCity2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+orders_res.getId()+"]: different values found for attribute 'Orders.shipCity': " + firstNotNull_ShipCity + " and " + shipCity2 + "." );
						}
						if (firstNotNull_ShipCity == null && shipCity2 != null) {
							firstNotNull_ShipCity = shipCity2;
						}
					}
					orders_res.setShipCity(firstNotNull_ShipCity);
					
					// attribute 'Orders.shipRegion'
					String firstNotNull_ShipRegion = Util.getStringValue(r.getAs("shipRegion"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String shipRegion2 = Util.getStringValue(r.getAs("shipRegion_" + i));
						if (firstNotNull_ShipRegion != null && shipRegion2 != null && !firstNotNull_ShipRegion.equals(shipRegion2)) {
							orders_res.addLogEvent("Data consistency problem for [Orders - id :"+orders_res.getId()+"]: different values found for attribute 'Orders.shipRegion': " + firstNotNull_ShipRegion + " and " + shipRegion2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+orders_res.getId()+"]: different values found for attribute 'Orders.shipRegion': " + firstNotNull_ShipRegion + " and " + shipRegion2 + "." );
						}
						if (firstNotNull_ShipRegion == null && shipRegion2 != null) {
							firstNotNull_ShipRegion = shipRegion2;
						}
					}
					orders_res.setShipRegion(firstNotNull_ShipRegion);
					
					// attribute 'Orders.shipPostalCode'
					String firstNotNull_ShipPostalCode = Util.getStringValue(r.getAs("shipPostalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String shipPostalCode2 = Util.getStringValue(r.getAs("shipPostalCode_" + i));
						if (firstNotNull_ShipPostalCode != null && shipPostalCode2 != null && !firstNotNull_ShipPostalCode.equals(shipPostalCode2)) {
							orders_res.addLogEvent("Data consistency problem for [Orders - id :"+orders_res.getId()+"]: different values found for attribute 'Orders.shipPostalCode': " + firstNotNull_ShipPostalCode + " and " + shipPostalCode2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+orders_res.getId()+"]: different values found for attribute 'Orders.shipPostalCode': " + firstNotNull_ShipPostalCode + " and " + shipPostalCode2 + "." );
						}
						if (firstNotNull_ShipPostalCode == null && shipPostalCode2 != null) {
							firstNotNull_ShipPostalCode = shipPostalCode2;
						}
					}
					orders_res.setShipPostalCode(firstNotNull_ShipPostalCode);
					
					// attribute 'Orders.shipCountry'
					String firstNotNull_ShipCountry = Util.getStringValue(r.getAs("shipCountry"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String shipCountry2 = Util.getStringValue(r.getAs("shipCountry_" + i));
						if (firstNotNull_ShipCountry != null && shipCountry2 != null && !firstNotNull_ShipCountry.equals(shipCountry2)) {
							orders_res.addLogEvent("Data consistency problem for [Orders - id :"+orders_res.getId()+"]: different values found for attribute 'Orders.shipCountry': " + firstNotNull_ShipCountry + " and " + shipCountry2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+orders_res.getId()+"]: different values found for attribute 'Orders.shipCountry': " + firstNotNull_ShipCountry + " and " + shipCountry2 + "." );
						}
						if (firstNotNull_ShipCountry == null && shipCountry2 != null) {
							firstNotNull_ShipCountry = shipCountry2;
						}
					}
					orders_res.setShipCountry(firstNotNull_ShipCountry);
	
					WrappedArray logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							orders_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							orders_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					return orders_res;
				}, Encoders.bean(Orders.class));
			return d;
	}
	
	
	
	
	
	
	
	public Dataset<Orders> getOrdersList(Orders.buy role, Customers customers) {
		if(role != null) {
			if(role.equals(Orders.buy.boughtOrder))
				return getBoughtOrderListInBuyByCustomer(customers);
		}
		return null;
	}
	
	public Dataset<Orders> getOrdersList(Orders.buy role, Condition<CustomersAttribute> condition) {
		if(role != null) {
			if(role.equals(Orders.buy.boughtOrder))
				return getBoughtOrderListInBuyByCustomerCondition(condition);
		}
		return null;
	}
	
	public Dataset<Orders> getOrdersList(Orders.buy role, Condition<OrdersAttribute> condition1, Condition<CustomersAttribute> condition2) {
		if(role != null) {
			if(role.equals(Orders.buy.boughtOrder))
				return getBoughtOrderListInBuy(condition1, condition2);
		}
		return null;
	}
	
	
	
	public Dataset<Orders> getOrdersList(Orders.register role, Employees employees) {
		if(role != null) {
			if(role.equals(Orders.register.processedOrder))
				return getProcessedOrderListInRegisterByEmployeeInCharge(employees);
		}
		return null;
	}
	
	public Dataset<Orders> getOrdersList(Orders.register role, Condition<EmployeesAttribute> condition) {
		if(role != null) {
			if(role.equals(Orders.register.processedOrder))
				return getProcessedOrderListInRegisterByEmployeeInChargeCondition(condition);
		}
		return null;
	}
	
	public Dataset<Orders> getOrdersList(Orders.register role, Condition<OrdersAttribute> condition1, Condition<EmployeesAttribute> condition2) {
		if(role != null) {
			if(role.equals(Orders.register.processedOrder))
				return getProcessedOrderListInRegister(condition1, condition2);
		}
		return null;
	}
	
	
	
	public Dataset<Orders> getOrdersList(Orders.ships role, Shippers shippers) {
		if(role != null) {
			if(role.equals(Orders.ships.shippedOrder))
				return getShippedOrderListInShipsByShipper(shippers);
		}
		return null;
	}
	
	public Dataset<Orders> getOrdersList(Orders.ships role, Condition<ShippersAttribute> condition) {
		if(role != null) {
			if(role.equals(Orders.ships.shippedOrder))
				return getShippedOrderListInShipsByShipperCondition(condition);
		}
		return null;
	}
	
	public Dataset<Orders> getOrdersList(Orders.ships role, Condition<OrdersAttribute> condition1, Condition<ShippersAttribute> condition2) {
		if(role != null) {
			if(role.equals(Orders.ships.shippedOrder))
				return getShippedOrderListInShips(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	public abstract Dataset<Orders> getBoughtOrderListInBuy(conditions.Condition<conditions.OrdersAttribute> boughtOrder_condition,conditions.Condition<conditions.CustomersAttribute> customer_condition);
	
	public Dataset<Orders> getBoughtOrderListInBuyByBoughtOrderCondition(conditions.Condition<conditions.OrdersAttribute> boughtOrder_condition){
		return getBoughtOrderListInBuy(boughtOrder_condition, null);
	}
	public Dataset<Orders> getBoughtOrderListInBuyByCustomerCondition(conditions.Condition<conditions.CustomersAttribute> customer_condition){
		return getBoughtOrderListInBuy(null, customer_condition);
	}
	
	public Dataset<Orders> getBoughtOrderListInBuyByCustomer(pojo.Customers customer){
		if(customer == null)
			return null;
	
		Condition c;
		c=Condition.simple(CustomersAttribute.customerID,Operator.EQUALS, customer.getCustomerID());
		Dataset<Orders> res = getBoughtOrderListInBuyByCustomerCondition(c);
		return res;
	}
	
	public abstract Dataset<Orders> getProcessedOrderListInRegister(conditions.Condition<conditions.OrdersAttribute> processedOrder_condition,conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition);
	
	public Dataset<Orders> getProcessedOrderListInRegisterByProcessedOrderCondition(conditions.Condition<conditions.OrdersAttribute> processedOrder_condition){
		return getProcessedOrderListInRegister(processedOrder_condition, null);
	}
	public Dataset<Orders> getProcessedOrderListInRegisterByEmployeeInChargeCondition(conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition){
		return getProcessedOrderListInRegister(null, employeeInCharge_condition);
	}
	
	public Dataset<Orders> getProcessedOrderListInRegisterByEmployeeInCharge(pojo.Employees employeeInCharge){
		if(employeeInCharge == null)
			return null;
	
		Condition c;
		c=Condition.simple(EmployeesAttribute.employeeID,Operator.EQUALS, employeeInCharge.getEmployeeID());
		Dataset<Orders> res = getProcessedOrderListInRegisterByEmployeeInChargeCondition(c);
		return res;
	}
	
	public abstract Dataset<Orders> getShippedOrderListInShips(conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition,conditions.Condition<conditions.ShippersAttribute> shipper_condition);
	
	public Dataset<Orders> getShippedOrderListInShipsByShippedOrderCondition(conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition){
		return getShippedOrderListInShips(shippedOrder_condition, null);
	}
	public Dataset<Orders> getShippedOrderListInShipsByShipperCondition(conditions.Condition<conditions.ShippersAttribute> shipper_condition){
		return getShippedOrderListInShips(null, shipper_condition);
	}
	
	public Dataset<Orders> getShippedOrderListInShipsByShipper(pojo.Shippers shipper){
		if(shipper == null)
			return null;
	
		Condition c;
		c=Condition.simple(ShippersAttribute.shipperID,Operator.EQUALS, shipper.getShipperID());
		Dataset<Orders> res = getShippedOrderListInShipsByShipperCondition(c);
		return res;
	}
	
	public abstract Dataset<Orders> getOrderListInComposedOf(conditions.Condition<conditions.OrdersAttribute> order_condition,conditions.Condition<conditions.ProductsAttribute> orderedProducts_condition, conditions.Condition<conditions.ComposedOfAttribute> composedOf_condition);
	
	public Dataset<Orders> getOrderListInComposedOfByOrderCondition(conditions.Condition<conditions.OrdersAttribute> order_condition){
		return getOrderListInComposedOf(order_condition, null, null);
	}
	public Dataset<Orders> getOrderListInComposedOfByOrderedProductsCondition(conditions.Condition<conditions.ProductsAttribute> orderedProducts_condition){
		return getOrderListInComposedOf(null, orderedProducts_condition, null);
	}
	
	public Dataset<Orders> getOrderListInComposedOfByOrderedProducts(pojo.Products orderedProducts){
		if(orderedProducts == null)
			return null;
	
		Condition c;
		c=Condition.simple(ProductsAttribute.productId,Operator.EQUALS, orderedProducts.getProductId());
		Dataset<Orders> res = getOrderListInComposedOfByOrderedProductsCondition(c);
		return res;
	}
	
	public Dataset<Orders> getOrderListInComposedOfByComposedOfCondition(
		conditions.Condition<conditions.ComposedOfAttribute> composedOf_condition
	){
		return getOrderListInComposedOf(null, null, composedOf_condition);
	}
	
	public abstract boolean insertOrders(
		Orders orders,
		Customers	customerBuy,
		Employees	employeeInChargeRegister,
		Shippers	shipperShips);
	
	public abstract boolean insertOrdersInOrdersFromMyMongoDB(Orders orders,
		Customers	customerBuy,
		Employees	employeeInChargeRegister,
		Shippers	shipperShips);
	public abstract boolean insertOrdersInCustomersPurchasedFromMyRedisDB(Orders orders,
		Customers	customerBuy,
		Employees	employeeInChargeRegister,
		Shippers	shipperShips);
	private boolean inUpdateMethod = false;
	private List<Row> allOrdersIdList = null;
	public abstract void updateOrdersList(conditions.Condition<conditions.OrdersAttribute> condition, conditions.SetClause<conditions.OrdersAttribute> set);
	
	public void updateOrders(pojo.Orders orders) {
		//TODO using the id
		return;
	}
	public abstract void updateBoughtOrderListInBuy(
		conditions.Condition<conditions.OrdersAttribute> boughtOrder_condition,
		conditions.Condition<conditions.CustomersAttribute> customer_condition,
		
		conditions.SetClause<conditions.OrdersAttribute> set
	);
	
	public void updateBoughtOrderListInBuyByBoughtOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> boughtOrder_condition,
		conditions.SetClause<conditions.OrdersAttribute> set
	){
		updateBoughtOrderListInBuy(boughtOrder_condition, null, set);
	}
	public void updateBoughtOrderListInBuyByCustomerCondition(
		conditions.Condition<conditions.CustomersAttribute> customer_condition,
		conditions.SetClause<conditions.OrdersAttribute> set
	){
		updateBoughtOrderListInBuy(null, customer_condition, set);
	}
	
	public void updateBoughtOrderListInBuyByCustomer(
		pojo.Customers customer,
		conditions.SetClause<conditions.OrdersAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public abstract void updateProcessedOrderListInRegister(
		conditions.Condition<conditions.OrdersAttribute> processedOrder_condition,
		conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition,
		
		conditions.SetClause<conditions.OrdersAttribute> set
	);
	
	public void updateProcessedOrderListInRegisterByProcessedOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> processedOrder_condition,
		conditions.SetClause<conditions.OrdersAttribute> set
	){
		updateProcessedOrderListInRegister(processedOrder_condition, null, set);
	}
	public void updateProcessedOrderListInRegisterByEmployeeInChargeCondition(
		conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition,
		conditions.SetClause<conditions.OrdersAttribute> set
	){
		updateProcessedOrderListInRegister(null, employeeInCharge_condition, set);
	}
	
	public void updateProcessedOrderListInRegisterByEmployeeInCharge(
		pojo.Employees employeeInCharge,
		conditions.SetClause<conditions.OrdersAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public abstract void updateShippedOrderListInShips(
		conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition,
		conditions.Condition<conditions.ShippersAttribute> shipper_condition,
		
		conditions.SetClause<conditions.OrdersAttribute> set
	);
	
	public void updateShippedOrderListInShipsByShippedOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition,
		conditions.SetClause<conditions.OrdersAttribute> set
	){
		updateShippedOrderListInShips(shippedOrder_condition, null, set);
	}
	public void updateShippedOrderListInShipsByShipperCondition(
		conditions.Condition<conditions.ShippersAttribute> shipper_condition,
		conditions.SetClause<conditions.OrdersAttribute> set
	){
		updateShippedOrderListInShips(null, shipper_condition, set);
	}
	
	public void updateShippedOrderListInShipsByShipper(
		pojo.Shippers shipper,
		conditions.SetClause<conditions.OrdersAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public abstract void updateOrderListInComposedOf(
		conditions.Condition<conditions.OrdersAttribute> order_condition,
		conditions.Condition<conditions.ProductsAttribute> orderedProducts_condition,
		conditions.Condition<conditions.ComposedOfAttribute> composedOf,
		conditions.SetClause<conditions.OrdersAttribute> set
	);
	
	public void updateOrderListInComposedOfByOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> order_condition,
		conditions.SetClause<conditions.OrdersAttribute> set
	){
		updateOrderListInComposedOf(order_condition, null, null, set);
	}
	public void updateOrderListInComposedOfByOrderedProductsCondition(
		conditions.Condition<conditions.ProductsAttribute> orderedProducts_condition,
		conditions.SetClause<conditions.OrdersAttribute> set
	){
		updateOrderListInComposedOf(null, orderedProducts_condition, null, set);
	}
	
	public void updateOrderListInComposedOfByOrderedProducts(
		pojo.Products orderedProducts,
		conditions.SetClause<conditions.OrdersAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateOrderListInComposedOfByComposedOfCondition(
		conditions.Condition<conditions.ComposedOfAttribute> composedOf_condition,
		conditions.SetClause<conditions.OrdersAttribute> set
	){
		updateOrderListInComposedOf(null, null, composedOf_condition, set);
	}
	
	
	public abstract void deleteOrdersList(conditions.Condition<conditions.OrdersAttribute> condition);
	
	public void deleteOrders(pojo.Orders orders) {
		//TODO using the id
		return;
	}
	public abstract void deleteBoughtOrderListInBuy(	
		conditions.Condition<conditions.OrdersAttribute> boughtOrder_condition,	
		conditions.Condition<conditions.CustomersAttribute> customer_condition);
	
	public void deleteBoughtOrderListInBuyByBoughtOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> boughtOrder_condition
	){
		deleteBoughtOrderListInBuy(boughtOrder_condition, null);
	}
	public void deleteBoughtOrderListInBuyByCustomerCondition(
		conditions.Condition<conditions.CustomersAttribute> customer_condition
	){
		deleteBoughtOrderListInBuy(null, customer_condition);
	}
	
	public void deleteBoughtOrderListInBuyByCustomer(
		pojo.Customers customer 
	){
		//TODO get id in condition
		return;	
	}
	
	public abstract void deleteProcessedOrderListInRegister(	
		conditions.Condition<conditions.OrdersAttribute> processedOrder_condition,	
		conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition);
	
	public void deleteProcessedOrderListInRegisterByProcessedOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> processedOrder_condition
	){
		deleteProcessedOrderListInRegister(processedOrder_condition, null);
	}
	public void deleteProcessedOrderListInRegisterByEmployeeInChargeCondition(
		conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition
	){
		deleteProcessedOrderListInRegister(null, employeeInCharge_condition);
	}
	
	public void deleteProcessedOrderListInRegisterByEmployeeInCharge(
		pojo.Employees employeeInCharge 
	){
		//TODO get id in condition
		return;	
	}
	
	public abstract void deleteShippedOrderListInShips(	
		conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition,	
		conditions.Condition<conditions.ShippersAttribute> shipper_condition);
	
	public void deleteShippedOrderListInShipsByShippedOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition
	){
		deleteShippedOrderListInShips(shippedOrder_condition, null);
	}
	public void deleteShippedOrderListInShipsByShipperCondition(
		conditions.Condition<conditions.ShippersAttribute> shipper_condition
	){
		deleteShippedOrderListInShips(null, shipper_condition);
	}
	
	public void deleteShippedOrderListInShipsByShipper(
		pojo.Shippers shipper 
	){
		//TODO get id in condition
		return;	
	}
	
	public abstract void deleteOrderListInComposedOf(	
		conditions.Condition<conditions.OrdersAttribute> order_condition,	
		conditions.Condition<conditions.ProductsAttribute> orderedProducts_condition,
		conditions.Condition<conditions.ComposedOfAttribute> composedOf);
	
	public void deleteOrderListInComposedOfByOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> order_condition
	){
		deleteOrderListInComposedOf(order_condition, null, null);
	}
	public void deleteOrderListInComposedOfByOrderedProductsCondition(
		conditions.Condition<conditions.ProductsAttribute> orderedProducts_condition
	){
		deleteOrderListInComposedOf(null, orderedProducts_condition, null);
	}
	
	public void deleteOrderListInComposedOfByOrderedProducts(
		pojo.Products orderedProducts 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteOrderListInComposedOfByComposedOfCondition(
		conditions.Condition<conditions.ComposedOfAttribute> composedOf_condition
	){
		deleteOrderListInComposedOf(null, null, composedOf_condition);
	}
	
}
