package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import pojo.Customers;
import conditions.CustomersAttribute;
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
import conditions.CustomersAttribute;
import pojo.Buy;
import conditions.OrdersAttribute;
import pojo.Orders;

public abstract class CustomersService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CustomersService.class);
	protected BuyService buyService = new dao.impl.BuyServiceImpl();
	


	public static enum ROLE_NAME {
		BUY_CUSTOMER
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.BUY_CUSTOMER, loading.Loading.LAZY);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public CustomersService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public CustomersService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
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
	
	
	public Dataset<Customers> getCustomersList(){
		return getCustomersList(null);
	}
	
	public Dataset<Customers> getCustomersList(conditions.Condition<conditions.CustomersAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Customers>> datasets = new ArrayList<Dataset<Customers>>();
		Dataset<Customers> d = null;
		d = getCustomersListInCustomersFromMyRedisDB(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		d = getCustomersListInCustomersPurchasedFromMyRedisDB(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsCustomers(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Customers>) r -> condition == null || condition.evaluate(r));
		d = d.dropDuplicates(new String[] {"customerID"});
		return d;
	}
	
	
	
	
	
	public abstract Dataset<Customers> getCustomersListInCustomersFromMyRedisDB(conditions.Condition<conditions.CustomersAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	
	public abstract Dataset<Customers> getCustomersListInCustomersPurchasedFromMyRedisDB(conditions.Condition<conditions.CustomersAttribute> condition, MutableBoolean refilterFlag);
	
	
	public Customers getCustomersById(String customerID){
		Condition cond;
		cond = Condition.simple(CustomersAttribute.customerID, conditions.Operator.EQUALS, customerID);
		Dataset<Customers> res = getCustomersList(cond);
		if(res!=null && !res.isEmpty())
			return res.first();
		return null;
	}
	
	public Dataset<Customers> getCustomersListByCustomerID(String customerID) {
		return getCustomersList(conditions.Condition.simple(conditions.CustomersAttribute.customerID, conditions.Operator.EQUALS, customerID));
	}
	
	public Dataset<Customers> getCustomersListByCompanyName(String companyName) {
		return getCustomersList(conditions.Condition.simple(conditions.CustomersAttribute.companyName, conditions.Operator.EQUALS, companyName));
	}
	
	public Dataset<Customers> getCustomersListByContactName(String contactName) {
		return getCustomersList(conditions.Condition.simple(conditions.CustomersAttribute.contactName, conditions.Operator.EQUALS, contactName));
	}
	
	public Dataset<Customers> getCustomersListByContactTitle(String contactTitle) {
		return getCustomersList(conditions.Condition.simple(conditions.CustomersAttribute.contactTitle, conditions.Operator.EQUALS, contactTitle));
	}
	
	public Dataset<Customers> getCustomersListByAddress(String address) {
		return getCustomersList(conditions.Condition.simple(conditions.CustomersAttribute.address, conditions.Operator.EQUALS, address));
	}
	
	public Dataset<Customers> getCustomersListByCity(String city) {
		return getCustomersList(conditions.Condition.simple(conditions.CustomersAttribute.city, conditions.Operator.EQUALS, city));
	}
	
	public Dataset<Customers> getCustomersListByRegion(String region) {
		return getCustomersList(conditions.Condition.simple(conditions.CustomersAttribute.region, conditions.Operator.EQUALS, region));
	}
	
	public Dataset<Customers> getCustomersListByPostalCode(String postalCode) {
		return getCustomersList(conditions.Condition.simple(conditions.CustomersAttribute.postalCode, conditions.Operator.EQUALS, postalCode));
	}
	
	public Dataset<Customers> getCustomersListByCountry(String country) {
		return getCustomersList(conditions.Condition.simple(conditions.CustomersAttribute.country, conditions.Operator.EQUALS, country));
	}
	
	public Dataset<Customers> getCustomersListByPhone(String phone) {
		return getCustomersList(conditions.Condition.simple(conditions.CustomersAttribute.phone, conditions.Operator.EQUALS, phone));
	}
	
	public Dataset<Customers> getCustomersListByFax(String fax) {
		return getCustomersList(conditions.Condition.simple(conditions.CustomersAttribute.fax, conditions.Operator.EQUALS, fax));
	}
	
	
	
	public static Dataset<Customers> fullOuterJoinsCustomers(List<Dataset<Customers>> datasetsPOJO) {
		return fullOuterJoinsCustomers(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Customers> fullLeftOuterJoinsCustomers(List<Dataset<Customers>> datasetsPOJO) {
		return fullOuterJoinsCustomers(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Customers> fullOuterJoinsCustomers(List<Dataset<Customers>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Customers> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("customerID");
			logger.debug("Start {} of [{}] datasets of [Customers] objects",joinMode,datasetsPOJO.size());
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("companyName", "companyName_1")
								.withColumnRenamed("contactName", "contactName_1")
								.withColumnRenamed("contactTitle", "contactTitle_1")
								.withColumnRenamed("address", "address_1")
								.withColumnRenamed("city", "city_1")
								.withColumnRenamed("region", "region_1")
								.withColumnRenamed("postalCode", "postalCode_1")
								.withColumnRenamed("country", "country_1")
								.withColumnRenamed("phone", "phone_1")
								.withColumnRenamed("fax", "fax_1")
								.withColumnRenamed("logEvents", "logEvents_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("companyName", "companyName_" + i)
								.withColumnRenamed("contactName", "contactName_" + i)
								.withColumnRenamed("contactTitle", "contactTitle_" + i)
								.withColumnRenamed("address", "address_" + i)
								.withColumnRenamed("city", "city_" + i)
								.withColumnRenamed("region", "region_" + i)
								.withColumnRenamed("postalCode", "postalCode_" + i)
								.withColumnRenamed("country", "country_" + i)
								.withColumnRenamed("phone", "phone_" + i)
								.withColumnRenamed("fax", "fax_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
						, seq, joinMode);
			}
			logger.debug("End join. Start");
			logger.debug("Start transforming Row objects to [Customers] objects"); 
			d = res.map((MapFunction<Row, Customers>) r -> {
					Customers customers_res = new Customers();
					
					// attribute 'Customers.customerID'
					String firstNotNull_customerID = Util.getStringValue(r.getAs("customerID"));
					customers_res.setCustomerID(firstNotNull_customerID);
					
					// attribute 'Customers.companyName'
					String firstNotNull_companyName = Util.getStringValue(r.getAs("companyName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String companyName2 = Util.getStringValue(r.getAs("companyName_" + i));
						if (firstNotNull_companyName != null && companyName2 != null && !firstNotNull_companyName.equals(companyName2)) {
							customers_res.addLogEvent("Data consistency problem for [Customers - id :"+customers_res.getCustomerID()+"]: different values found for attribute 'Customers.companyName': " + firstNotNull_companyName + " and " + companyName2 + "." );
							logger.warn("Data consistency problem for [Customers - id :"+customers_res.getCustomerID()+"]: different values found for attribute 'Customers.companyName': " + firstNotNull_companyName + " and " + companyName2 + "." );
						}
						if (firstNotNull_companyName == null && companyName2 != null) {
							firstNotNull_companyName = companyName2;
						}
					}
					customers_res.setCompanyName(firstNotNull_companyName);
					
					// attribute 'Customers.contactName'
					String firstNotNull_contactName = Util.getStringValue(r.getAs("contactName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String contactName2 = Util.getStringValue(r.getAs("contactName_" + i));
						if (firstNotNull_contactName != null && contactName2 != null && !firstNotNull_contactName.equals(contactName2)) {
							customers_res.addLogEvent("Data consistency problem for [Customers - id :"+customers_res.getCustomerID()+"]: different values found for attribute 'Customers.contactName': " + firstNotNull_contactName + " and " + contactName2 + "." );
							logger.warn("Data consistency problem for [Customers - id :"+customers_res.getCustomerID()+"]: different values found for attribute 'Customers.contactName': " + firstNotNull_contactName + " and " + contactName2 + "." );
						}
						if (firstNotNull_contactName == null && contactName2 != null) {
							firstNotNull_contactName = contactName2;
						}
					}
					customers_res.setContactName(firstNotNull_contactName);
					
					// attribute 'Customers.contactTitle'
					String firstNotNull_contactTitle = Util.getStringValue(r.getAs("contactTitle"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String contactTitle2 = Util.getStringValue(r.getAs("contactTitle_" + i));
						if (firstNotNull_contactTitle != null && contactTitle2 != null && !firstNotNull_contactTitle.equals(contactTitle2)) {
							customers_res.addLogEvent("Data consistency problem for [Customers - id :"+customers_res.getCustomerID()+"]: different values found for attribute 'Customers.contactTitle': " + firstNotNull_contactTitle + " and " + contactTitle2 + "." );
							logger.warn("Data consistency problem for [Customers - id :"+customers_res.getCustomerID()+"]: different values found for attribute 'Customers.contactTitle': " + firstNotNull_contactTitle + " and " + contactTitle2 + "." );
						}
						if (firstNotNull_contactTitle == null && contactTitle2 != null) {
							firstNotNull_contactTitle = contactTitle2;
						}
					}
					customers_res.setContactTitle(firstNotNull_contactTitle);
					
					// attribute 'Customers.address'
					String firstNotNull_address = Util.getStringValue(r.getAs("address"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String address2 = Util.getStringValue(r.getAs("address_" + i));
						if (firstNotNull_address != null && address2 != null && !firstNotNull_address.equals(address2)) {
							customers_res.addLogEvent("Data consistency problem for [Customers - id :"+customers_res.getCustomerID()+"]: different values found for attribute 'Customers.address': " + firstNotNull_address + " and " + address2 + "." );
							logger.warn("Data consistency problem for [Customers - id :"+customers_res.getCustomerID()+"]: different values found for attribute 'Customers.address': " + firstNotNull_address + " and " + address2 + "." );
						}
						if (firstNotNull_address == null && address2 != null) {
							firstNotNull_address = address2;
						}
					}
					customers_res.setAddress(firstNotNull_address);
					
					// attribute 'Customers.city'
					String firstNotNull_city = Util.getStringValue(r.getAs("city"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String city2 = Util.getStringValue(r.getAs("city_" + i));
						if (firstNotNull_city != null && city2 != null && !firstNotNull_city.equals(city2)) {
							customers_res.addLogEvent("Data consistency problem for [Customers - id :"+customers_res.getCustomerID()+"]: different values found for attribute 'Customers.city': " + firstNotNull_city + " and " + city2 + "." );
							logger.warn("Data consistency problem for [Customers - id :"+customers_res.getCustomerID()+"]: different values found for attribute 'Customers.city': " + firstNotNull_city + " and " + city2 + "." );
						}
						if (firstNotNull_city == null && city2 != null) {
							firstNotNull_city = city2;
						}
					}
					customers_res.setCity(firstNotNull_city);
					
					// attribute 'Customers.region'
					String firstNotNull_region = Util.getStringValue(r.getAs("region"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String region2 = Util.getStringValue(r.getAs("region_" + i));
						if (firstNotNull_region != null && region2 != null && !firstNotNull_region.equals(region2)) {
							customers_res.addLogEvent("Data consistency problem for [Customers - id :"+customers_res.getCustomerID()+"]: different values found for attribute 'Customers.region': " + firstNotNull_region + " and " + region2 + "." );
							logger.warn("Data consistency problem for [Customers - id :"+customers_res.getCustomerID()+"]: different values found for attribute 'Customers.region': " + firstNotNull_region + " and " + region2 + "." );
						}
						if (firstNotNull_region == null && region2 != null) {
							firstNotNull_region = region2;
						}
					}
					customers_res.setRegion(firstNotNull_region);
					
					// attribute 'Customers.postalCode'
					String firstNotNull_postalCode = Util.getStringValue(r.getAs("postalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String postalCode2 = Util.getStringValue(r.getAs("postalCode_" + i));
						if (firstNotNull_postalCode != null && postalCode2 != null && !firstNotNull_postalCode.equals(postalCode2)) {
							customers_res.addLogEvent("Data consistency problem for [Customers - id :"+customers_res.getCustomerID()+"]: different values found for attribute 'Customers.postalCode': " + firstNotNull_postalCode + " and " + postalCode2 + "." );
							logger.warn("Data consistency problem for [Customers - id :"+customers_res.getCustomerID()+"]: different values found for attribute 'Customers.postalCode': " + firstNotNull_postalCode + " and " + postalCode2 + "." );
						}
						if (firstNotNull_postalCode == null && postalCode2 != null) {
							firstNotNull_postalCode = postalCode2;
						}
					}
					customers_res.setPostalCode(firstNotNull_postalCode);
					
					// attribute 'Customers.country'
					String firstNotNull_country = Util.getStringValue(r.getAs("country"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String country2 = Util.getStringValue(r.getAs("country_" + i));
						if (firstNotNull_country != null && country2 != null && !firstNotNull_country.equals(country2)) {
							customers_res.addLogEvent("Data consistency problem for [Customers - id :"+customers_res.getCustomerID()+"]: different values found for attribute 'Customers.country': " + firstNotNull_country + " and " + country2 + "." );
							logger.warn("Data consistency problem for [Customers - id :"+customers_res.getCustomerID()+"]: different values found for attribute 'Customers.country': " + firstNotNull_country + " and " + country2 + "." );
						}
						if (firstNotNull_country == null && country2 != null) {
							firstNotNull_country = country2;
						}
					}
					customers_res.setCountry(firstNotNull_country);
					
					// attribute 'Customers.phone'
					String firstNotNull_phone = Util.getStringValue(r.getAs("phone"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String phone2 = Util.getStringValue(r.getAs("phone_" + i));
						if (firstNotNull_phone != null && phone2 != null && !firstNotNull_phone.equals(phone2)) {
							customers_res.addLogEvent("Data consistency problem for [Customers - id :"+customers_res.getCustomerID()+"]: different values found for attribute 'Customers.phone': " + firstNotNull_phone + " and " + phone2 + "." );
							logger.warn("Data consistency problem for [Customers - id :"+customers_res.getCustomerID()+"]: different values found for attribute 'Customers.phone': " + firstNotNull_phone + " and " + phone2 + "." );
						}
						if (firstNotNull_phone == null && phone2 != null) {
							firstNotNull_phone = phone2;
						}
					}
					customers_res.setPhone(firstNotNull_phone);
					
					// attribute 'Customers.fax'
					String firstNotNull_fax = Util.getStringValue(r.getAs("fax"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String fax2 = Util.getStringValue(r.getAs("fax_" + i));
						if (firstNotNull_fax != null && fax2 != null && !firstNotNull_fax.equals(fax2)) {
							customers_res.addLogEvent("Data consistency problem for [Customers - id :"+customers_res.getCustomerID()+"]: different values found for attribute 'Customers.fax': " + firstNotNull_fax + " and " + fax2 + "." );
							logger.warn("Data consistency problem for [Customers - id :"+customers_res.getCustomerID()+"]: different values found for attribute 'Customers.fax': " + firstNotNull_fax + " and " + fax2 + "." );
						}
						if (firstNotNull_fax == null && fax2 != null) {
							firstNotNull_fax = fax2;
						}
					}
					customers_res.setFax(firstNotNull_fax);
	
					WrappedArray logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							customers_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							customers_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					return customers_res;
				}, Encoders.bean(Customers.class));
			return d;
	}
	
	
	
	
	
	
	
	public Customers getCustomers(Customers.buy role, Orders orders) {
		if(role != null) {
			if(role.equals(Customers.buy.customer))
				return getCustomerInBuyByBoughtOrder(orders);
		}
		return null;
	}
	
	public Dataset<Customers> getCustomersList(Customers.buy role, Condition<OrdersAttribute> condition) {
		if(role != null) {
			if(role.equals(Customers.buy.customer))
				return getCustomerListInBuyByBoughtOrderCondition(condition);
		}
		return null;
	}
	
	public Dataset<Customers> getCustomersList(Customers.buy role, Condition<OrdersAttribute> condition1, Condition<CustomersAttribute> condition2) {
		if(role != null) {
			if(role.equals(Customers.buy.customer))
				return getCustomerListInBuy(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	
	
	public abstract Dataset<Customers> getCustomerListInBuy(conditions.Condition<conditions.OrdersAttribute> boughtOrder_condition,conditions.Condition<conditions.CustomersAttribute> customer_condition);
	
	public Dataset<Customers> getCustomerListInBuyByBoughtOrderCondition(conditions.Condition<conditions.OrdersAttribute> boughtOrder_condition){
		return getCustomerListInBuy(boughtOrder_condition, null);
	}
	
	public Customers getCustomerInBuyByBoughtOrder(pojo.Orders boughtOrder){
		if(boughtOrder == null)
			return null;
	
		Condition c;
		c=Condition.simple(OrdersAttribute.id,Operator.EQUALS, boughtOrder.getId());
		Dataset<Customers> res = getCustomerListInBuyByBoughtOrderCondition(c);
		return !res.isEmpty()?res.first():null;
	}
	
	public Dataset<Customers> getCustomerListInBuyByCustomerCondition(conditions.Condition<conditions.CustomersAttribute> customer_condition){
		return getCustomerListInBuy(null, customer_condition);
	}
	
	
	public abstract boolean insertCustomers(Customers customers);
	
	public abstract boolean insertCustomersInCustomersFromMyRedisDB(Customers customers); 
	public abstract boolean insertCustomersInCustomersPurchasedFromMyRedisDB(Customers customers); 
	private boolean inUpdateMethod = false;
	private List<Row> allCustomersIdList = null;
	public abstract void updateCustomersList(conditions.Condition<conditions.CustomersAttribute> condition, conditions.SetClause<conditions.CustomersAttribute> set);
	
	public void updateCustomers(pojo.Customers customers) {
		//TODO using the id
		return;
	}
	public abstract void updateCustomerListInBuy(
		conditions.Condition<conditions.OrdersAttribute> boughtOrder_condition,
		conditions.Condition<conditions.CustomersAttribute> customer_condition,
		
		conditions.SetClause<conditions.CustomersAttribute> set
	);
	
	public void updateCustomerListInBuyByBoughtOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> boughtOrder_condition,
		conditions.SetClause<conditions.CustomersAttribute> set
	){
		updateCustomerListInBuy(boughtOrder_condition, null, set);
	}
	
	public void updateCustomerInBuyByBoughtOrder(
		pojo.Orders boughtOrder,
		conditions.SetClause<conditions.CustomersAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateCustomerListInBuyByCustomerCondition(
		conditions.Condition<conditions.CustomersAttribute> customer_condition,
		conditions.SetClause<conditions.CustomersAttribute> set
	){
		updateCustomerListInBuy(null, customer_condition, set);
	}
	
	
	public abstract void deleteCustomersList(conditions.Condition<conditions.CustomersAttribute> condition);
	
	public void deleteCustomers(pojo.Customers customers) {
		//TODO using the id
		return;
	}
	public abstract void deleteCustomerListInBuy(	
		conditions.Condition<conditions.OrdersAttribute> boughtOrder_condition,	
		conditions.Condition<conditions.CustomersAttribute> customer_condition);
	
	public void deleteCustomerListInBuyByBoughtOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> boughtOrder_condition
	){
		deleteCustomerListInBuy(boughtOrder_condition, null);
	}
	
	public void deleteCustomerInBuyByBoughtOrder(
		pojo.Orders boughtOrder 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteCustomerListInBuyByCustomerCondition(
		conditions.Condition<conditions.CustomersAttribute> customer_condition
	){
		deleteCustomerListInBuy(null, customer_condition);
	}
	
}
