package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import pojo.Shippers;
import conditions.ShippersAttribute;
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
import conditions.ShippersAttribute;
import pojo.Ships;
import conditions.OrdersAttribute;
import pojo.Orders;

public abstract class ShippersService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ShippersService.class);
	protected ShipsService shipsService = new dao.impl.ShipsServiceImpl();
	


	public static enum ROLE_NAME {
		SHIPS_SHIPPER
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.SHIPS_SHIPPER, loading.Loading.LAZY);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public ShippersService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public ShippersService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
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
	
	
	public Dataset<Shippers> getShippersList(){
		return getShippersList(null);
	}
	
	public Dataset<Shippers> getShippersList(conditions.Condition<conditions.ShippersAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Shippers>> datasets = new ArrayList<Dataset<Shippers>>();
		Dataset<Shippers> d = null;
		d = getShippersListInOrdersFromMyMongoDB(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsShippers(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Shippers>) r -> condition == null || condition.evaluate(r));
		d = d.dropDuplicates(new String[] {"shipperID"});
		return d;
	}
	
	
	
	
	
	public abstract Dataset<Shippers> getShippersListInOrdersFromMyMongoDB(conditions.Condition<conditions.ShippersAttribute> condition, MutableBoolean refilterFlag);
	
	
	public Shippers getShippersById(Integer shipperID){
		Condition cond;
		cond = Condition.simple(ShippersAttribute.shipperID, conditions.Operator.EQUALS, shipperID);
		Dataset<Shippers> res = getShippersList(cond);
		if(res!=null && !res.isEmpty())
			return res.first();
		return null;
	}
	
	public Dataset<Shippers> getShippersListByShipperID(Integer shipperID) {
		return getShippersList(conditions.Condition.simple(conditions.ShippersAttribute.shipperID, conditions.Operator.EQUALS, shipperID));
	}
	
	public Dataset<Shippers> getShippersListByCompanyName(String CompanyName) {
		return getShippersList(conditions.Condition.simple(conditions.ShippersAttribute.CompanyName, conditions.Operator.EQUALS, CompanyName));
	}
	
	public Dataset<Shippers> getShippersListByPhone(String Phone) {
		return getShippersList(conditions.Condition.simple(conditions.ShippersAttribute.Phone, conditions.Operator.EQUALS, Phone));
	}
	
	
	
	public static Dataset<Shippers> fullOuterJoinsShippers(List<Dataset<Shippers>> datasetsPOJO) {
		return fullOuterJoinsShippers(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Shippers> fullLeftOuterJoinsShippers(List<Dataset<Shippers>> datasetsPOJO) {
		return fullOuterJoinsShippers(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Shippers> fullOuterJoinsShippers(List<Dataset<Shippers>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Shippers> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("shipperID");
			logger.debug("Start {} of [{}] datasets of [Shippers] objects",joinMode,datasetsPOJO.size());
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("companyName", "companyName_1")
								.withColumnRenamed("phone", "phone_1")
								.withColumnRenamed("logEvents", "logEvents_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("companyName", "companyName_" + i)
								.withColumnRenamed("phone", "phone_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
						, seq, joinMode);
			}
			logger.debug("End join. Start");
			logger.debug("Start transforming Row objects to [Shippers] objects"); 
			d = res.map((MapFunction<Row, Shippers>) r -> {
					Shippers shippers_res = new Shippers();
					
					// attribute 'Shippers.shipperID'
					Integer firstNotNull_shipperID = Util.getIntegerValue(r.getAs("shipperID"));
					shippers_res.setShipperID(firstNotNull_shipperID);
					
					// attribute 'Shippers.companyName'
					String firstNotNull_CompanyName = Util.getStringValue(r.getAs("companyName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String companyName2 = Util.getStringValue(r.getAs("companyName_" + i));
						if (firstNotNull_CompanyName != null && companyName2 != null && !firstNotNull_CompanyName.equals(companyName2)) {
							shippers_res.addLogEvent("Data consistency problem for [Shippers - id :"+shippers_res.getShipperID()+"]: different values found for attribute 'Shippers.companyName': " + firstNotNull_CompanyName + " and " + companyName2 + "." );
							logger.warn("Data consistency problem for [Shippers - id :"+shippers_res.getShipperID()+"]: different values found for attribute 'Shippers.companyName': " + firstNotNull_CompanyName + " and " + companyName2 + "." );
						}
						if (firstNotNull_CompanyName == null && companyName2 != null) {
							firstNotNull_CompanyName = companyName2;
						}
					}
					shippers_res.setCompanyName(firstNotNull_CompanyName);
					
					// attribute 'Shippers.phone'
					String firstNotNull_Phone = Util.getStringValue(r.getAs("phone"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String phone2 = Util.getStringValue(r.getAs("phone_" + i));
						if (firstNotNull_Phone != null && phone2 != null && !firstNotNull_Phone.equals(phone2)) {
							shippers_res.addLogEvent("Data consistency problem for [Shippers - id :"+shippers_res.getShipperID()+"]: different values found for attribute 'Shippers.phone': " + firstNotNull_Phone + " and " + phone2 + "." );
							logger.warn("Data consistency problem for [Shippers - id :"+shippers_res.getShipperID()+"]: different values found for attribute 'Shippers.phone': " + firstNotNull_Phone + " and " + phone2 + "." );
						}
						if (firstNotNull_Phone == null && phone2 != null) {
							firstNotNull_Phone = phone2;
						}
					}
					shippers_res.setPhone(firstNotNull_Phone);
	
					WrappedArray logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							shippers_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							shippers_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					return shippers_res;
				}, Encoders.bean(Shippers.class));
			return d;
	}
	
	
	
	
	
	
	
	
	
	public Shippers getShippers(Shippers.ships role, Orders orders) {
		if(role != null) {
			if(role.equals(Shippers.ships.shipper))
				return getShipperInShipsByShippedOrder(orders);
		}
		return null;
	}
	
	public Dataset<Shippers> getShippersList(Shippers.ships role, Condition<OrdersAttribute> condition) {
		if(role != null) {
			if(role.equals(Shippers.ships.shipper))
				return getShipperListInShipsByShippedOrderCondition(condition);
		}
		return null;
	}
	
	public Dataset<Shippers> getShippersList(Shippers.ships role, Condition<OrdersAttribute> condition1, Condition<ShippersAttribute> condition2) {
		if(role != null) {
			if(role.equals(Shippers.ships.shipper))
				return getShipperListInShips(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	public abstract Dataset<Shippers> getShipperListInShips(conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition,conditions.Condition<conditions.ShippersAttribute> shipper_condition);
	
	public Dataset<Shippers> getShipperListInShipsByShippedOrderCondition(conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition){
		return getShipperListInShips(shippedOrder_condition, null);
	}
	
	public Shippers getShipperInShipsByShippedOrder(pojo.Orders shippedOrder){
		if(shippedOrder == null)
			return null;
	
		Condition c;
		c=Condition.simple(OrdersAttribute.id,Operator.EQUALS, shippedOrder.getId());
		Dataset<Shippers> res = getShipperListInShipsByShippedOrderCondition(c);
		return !res.isEmpty()?res.first():null;
	}
	
	public Dataset<Shippers> getShipperListInShipsByShipperCondition(conditions.Condition<conditions.ShippersAttribute> shipper_condition){
		return getShipperListInShips(null, shipper_condition);
	}
	
	
	public abstract boolean insertShippers(Shippers shippers);
	
	private boolean inUpdateMethod = false;
	private List<Row> allShippersIdList = null;
	public abstract void updateShippersList(conditions.Condition<conditions.ShippersAttribute> condition, conditions.SetClause<conditions.ShippersAttribute> set);
	
	public void updateShippers(pojo.Shippers shippers) {
		//TODO using the id
		return;
	}
	public abstract void updateShipperListInShips(
		conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition,
		conditions.Condition<conditions.ShippersAttribute> shipper_condition,
		
		conditions.SetClause<conditions.ShippersAttribute> set
	);
	
	public void updateShipperListInShipsByShippedOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition,
		conditions.SetClause<conditions.ShippersAttribute> set
	){
		updateShipperListInShips(shippedOrder_condition, null, set);
	}
	
	public void updateShipperInShipsByShippedOrder(
		pojo.Orders shippedOrder,
		conditions.SetClause<conditions.ShippersAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateShipperListInShipsByShipperCondition(
		conditions.Condition<conditions.ShippersAttribute> shipper_condition,
		conditions.SetClause<conditions.ShippersAttribute> set
	){
		updateShipperListInShips(null, shipper_condition, set);
	}
	
	
	public abstract void deleteShippersList(conditions.Condition<conditions.ShippersAttribute> condition);
	
	public void deleteShippers(pojo.Shippers shippers) {
		//TODO using the id
		return;
	}
	public abstract void deleteShipperListInShips(	
		conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition,	
		conditions.Condition<conditions.ShippersAttribute> shipper_condition);
	
	public void deleteShipperListInShipsByShippedOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> shippedOrder_condition
	){
		deleteShipperListInShips(shippedOrder_condition, null);
	}
	
	public void deleteShipperInShipsByShippedOrder(
		pojo.Orders shippedOrder 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteShipperListInShipsByShipperCondition(
		conditions.Condition<conditions.ShippersAttribute> shipper_condition
	){
		deleteShipperListInShips(null, shipper_condition);
	}
	
}
