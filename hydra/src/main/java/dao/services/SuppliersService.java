package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import pojo.Suppliers;
import conditions.SuppliersAttribute;
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
import conditions.SuppliersAttribute;
import pojo.Supply;
import conditions.ProductsAttribute;
import pojo.Products;

public abstract class SuppliersService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SuppliersService.class);
	protected SupplyService supplyService = new dao.impl.SupplyServiceImpl();
	


	public static enum ROLE_NAME {
		SUPPLY_SUPPLIER
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.SUPPLY_SUPPLIER, loading.Loading.LAZY);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public SuppliersService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public SuppliersService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
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
	
	
	public Dataset<Suppliers> getSuppliersList(){
		return getSuppliersList(null);
	}
	
	public Dataset<Suppliers> getSuppliersList(conditions.Condition<conditions.SuppliersAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Suppliers>> datasets = new ArrayList<Dataset<Suppliers>>();
		Dataset<Suppliers> d = null;
		d = getSuppliersListInSuppliersFromMyRedisDB(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsSuppliers(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Suppliers>) r -> condition == null || condition.evaluate(r));
		d = d.dropDuplicates(new String[] {"supplierId"});
		return d;
	}
	
	
	
	
	
	public abstract Dataset<Suppliers> getSuppliersListInSuppliersFromMyRedisDB(conditions.Condition<conditions.SuppliersAttribute> condition, MutableBoolean refilterFlag);
	
	
	public Suppliers getSuppliersById(Integer supplierId){
		Condition cond;
		cond = Condition.simple(SuppliersAttribute.supplierId, conditions.Operator.EQUALS, supplierId);
		Dataset<Suppliers> res = getSuppliersList(cond);
		if(res!=null && !res.isEmpty())
			return res.first();
		return null;
	}
	
	public Dataset<Suppliers> getSuppliersListBySupplierId(Integer supplierId) {
		return getSuppliersList(conditions.Condition.simple(conditions.SuppliersAttribute.supplierId, conditions.Operator.EQUALS, supplierId));
	}
	
	public Dataset<Suppliers> getSuppliersListByCompanyName(String CompanyName) {
		return getSuppliersList(conditions.Condition.simple(conditions.SuppliersAttribute.CompanyName, conditions.Operator.EQUALS, CompanyName));
	}
	
	public Dataset<Suppliers> getSuppliersListByContactName(String ContactName) {
		return getSuppliersList(conditions.Condition.simple(conditions.SuppliersAttribute.ContactName, conditions.Operator.EQUALS, ContactName));
	}
	
	public Dataset<Suppliers> getSuppliersListByContactTitle(String ContactTitle) {
		return getSuppliersList(conditions.Condition.simple(conditions.SuppliersAttribute.ContactTitle, conditions.Operator.EQUALS, ContactTitle));
	}
	
	public Dataset<Suppliers> getSuppliersListByAddress(String Address) {
		return getSuppliersList(conditions.Condition.simple(conditions.SuppliersAttribute.Address, conditions.Operator.EQUALS, Address));
	}
	
	public Dataset<Suppliers> getSuppliersListByCity(String City) {
		return getSuppliersList(conditions.Condition.simple(conditions.SuppliersAttribute.City, conditions.Operator.EQUALS, City));
	}
	
	public Dataset<Suppliers> getSuppliersListByRegion(String Region) {
		return getSuppliersList(conditions.Condition.simple(conditions.SuppliersAttribute.Region, conditions.Operator.EQUALS, Region));
	}
	
	public Dataset<Suppliers> getSuppliersListByPostalCode(String PostalCode) {
		return getSuppliersList(conditions.Condition.simple(conditions.SuppliersAttribute.PostalCode, conditions.Operator.EQUALS, PostalCode));
	}
	
	public Dataset<Suppliers> getSuppliersListByCountry(String Country) {
		return getSuppliersList(conditions.Condition.simple(conditions.SuppliersAttribute.Country, conditions.Operator.EQUALS, Country));
	}
	
	public Dataset<Suppliers> getSuppliersListByPhone(String Phone) {
		return getSuppliersList(conditions.Condition.simple(conditions.SuppliersAttribute.Phone, conditions.Operator.EQUALS, Phone));
	}
	
	public Dataset<Suppliers> getSuppliersListByFax(String Fax) {
		return getSuppliersList(conditions.Condition.simple(conditions.SuppliersAttribute.Fax, conditions.Operator.EQUALS, Fax));
	}
	
	public Dataset<Suppliers> getSuppliersListByHomePage(String HomePage) {
		return getSuppliersList(conditions.Condition.simple(conditions.SuppliersAttribute.HomePage, conditions.Operator.EQUALS, HomePage));
	}
	
	
	
	public static Dataset<Suppliers> fullOuterJoinsSuppliers(List<Dataset<Suppliers>> datasetsPOJO) {
		return fullOuterJoinsSuppliers(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Suppliers> fullLeftOuterJoinsSuppliers(List<Dataset<Suppliers>> datasetsPOJO) {
		return fullOuterJoinsSuppliers(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Suppliers> fullOuterJoinsSuppliers(List<Dataset<Suppliers>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Suppliers> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("supplierId");
			logger.debug("Start {} of [{}] datasets of [Suppliers] objects",joinMode,datasetsPOJO.size());
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
								.withColumnRenamed("homePage", "homePage_1")
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
								.withColumnRenamed("homePage", "homePage_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
						, seq, joinMode);
			}
			logger.debug("End join. Start");
			logger.debug("Start transforming Row objects to [Suppliers] objects"); 
			d = res.map((MapFunction<Row, Suppliers>) r -> {
					Suppliers suppliers_res = new Suppliers();
					
					// attribute 'Suppliers.supplierId'
					Integer firstNotNull_supplierId = Util.getIntegerValue(r.getAs("supplierId"));
					suppliers_res.setSupplierId(firstNotNull_supplierId);
					
					// attribute 'Suppliers.companyName'
					String firstNotNull_CompanyName = Util.getStringValue(r.getAs("companyName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String companyName2 = Util.getStringValue(r.getAs("companyName_" + i));
						if (firstNotNull_CompanyName != null && companyName2 != null && !firstNotNull_CompanyName.equals(companyName2)) {
							suppliers_res.addLogEvent("Data consistency problem for [Suppliers - id :"+suppliers_res.getSupplierId()+"]: different values found for attribute 'Suppliers.companyName': " + firstNotNull_CompanyName + " and " + companyName2 + "." );
							logger.warn("Data consistency problem for [Suppliers - id :"+suppliers_res.getSupplierId()+"]: different values found for attribute 'Suppliers.companyName': " + firstNotNull_CompanyName + " and " + companyName2 + "." );
						}
						if (firstNotNull_CompanyName == null && companyName2 != null) {
							firstNotNull_CompanyName = companyName2;
						}
					}
					suppliers_res.setCompanyName(firstNotNull_CompanyName);
					
					// attribute 'Suppliers.contactName'
					String firstNotNull_ContactName = Util.getStringValue(r.getAs("contactName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String contactName2 = Util.getStringValue(r.getAs("contactName_" + i));
						if (firstNotNull_ContactName != null && contactName2 != null && !firstNotNull_ContactName.equals(contactName2)) {
							suppliers_res.addLogEvent("Data consistency problem for [Suppliers - id :"+suppliers_res.getSupplierId()+"]: different values found for attribute 'Suppliers.contactName': " + firstNotNull_ContactName + " and " + contactName2 + "." );
							logger.warn("Data consistency problem for [Suppliers - id :"+suppliers_res.getSupplierId()+"]: different values found for attribute 'Suppliers.contactName': " + firstNotNull_ContactName + " and " + contactName2 + "." );
						}
						if (firstNotNull_ContactName == null && contactName2 != null) {
							firstNotNull_ContactName = contactName2;
						}
					}
					suppliers_res.setContactName(firstNotNull_ContactName);
					
					// attribute 'Suppliers.contactTitle'
					String firstNotNull_ContactTitle = Util.getStringValue(r.getAs("contactTitle"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String contactTitle2 = Util.getStringValue(r.getAs("contactTitle_" + i));
						if (firstNotNull_ContactTitle != null && contactTitle2 != null && !firstNotNull_ContactTitle.equals(contactTitle2)) {
							suppliers_res.addLogEvent("Data consistency problem for [Suppliers - id :"+suppliers_res.getSupplierId()+"]: different values found for attribute 'Suppliers.contactTitle': " + firstNotNull_ContactTitle + " and " + contactTitle2 + "." );
							logger.warn("Data consistency problem for [Suppliers - id :"+suppliers_res.getSupplierId()+"]: different values found for attribute 'Suppliers.contactTitle': " + firstNotNull_ContactTitle + " and " + contactTitle2 + "." );
						}
						if (firstNotNull_ContactTitle == null && contactTitle2 != null) {
							firstNotNull_ContactTitle = contactTitle2;
						}
					}
					suppliers_res.setContactTitle(firstNotNull_ContactTitle);
					
					// attribute 'Suppliers.address'
					String firstNotNull_Address = Util.getStringValue(r.getAs("address"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String address2 = Util.getStringValue(r.getAs("address_" + i));
						if (firstNotNull_Address != null && address2 != null && !firstNotNull_Address.equals(address2)) {
							suppliers_res.addLogEvent("Data consistency problem for [Suppliers - id :"+suppliers_res.getSupplierId()+"]: different values found for attribute 'Suppliers.address': " + firstNotNull_Address + " and " + address2 + "." );
							logger.warn("Data consistency problem for [Suppliers - id :"+suppliers_res.getSupplierId()+"]: different values found for attribute 'Suppliers.address': " + firstNotNull_Address + " and " + address2 + "." );
						}
						if (firstNotNull_Address == null && address2 != null) {
							firstNotNull_Address = address2;
						}
					}
					suppliers_res.setAddress(firstNotNull_Address);
					
					// attribute 'Suppliers.city'
					String firstNotNull_City = Util.getStringValue(r.getAs("city"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String city2 = Util.getStringValue(r.getAs("city_" + i));
						if (firstNotNull_City != null && city2 != null && !firstNotNull_City.equals(city2)) {
							suppliers_res.addLogEvent("Data consistency problem for [Suppliers - id :"+suppliers_res.getSupplierId()+"]: different values found for attribute 'Suppliers.city': " + firstNotNull_City + " and " + city2 + "." );
							logger.warn("Data consistency problem for [Suppliers - id :"+suppliers_res.getSupplierId()+"]: different values found for attribute 'Suppliers.city': " + firstNotNull_City + " and " + city2 + "." );
						}
						if (firstNotNull_City == null && city2 != null) {
							firstNotNull_City = city2;
						}
					}
					suppliers_res.setCity(firstNotNull_City);
					
					// attribute 'Suppliers.region'
					String firstNotNull_Region = Util.getStringValue(r.getAs("region"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String region2 = Util.getStringValue(r.getAs("region_" + i));
						if (firstNotNull_Region != null && region2 != null && !firstNotNull_Region.equals(region2)) {
							suppliers_res.addLogEvent("Data consistency problem for [Suppliers - id :"+suppliers_res.getSupplierId()+"]: different values found for attribute 'Suppliers.region': " + firstNotNull_Region + " and " + region2 + "." );
							logger.warn("Data consistency problem for [Suppliers - id :"+suppliers_res.getSupplierId()+"]: different values found for attribute 'Suppliers.region': " + firstNotNull_Region + " and " + region2 + "." );
						}
						if (firstNotNull_Region == null && region2 != null) {
							firstNotNull_Region = region2;
						}
					}
					suppliers_res.setRegion(firstNotNull_Region);
					
					// attribute 'Suppliers.postalCode'
					String firstNotNull_PostalCode = Util.getStringValue(r.getAs("postalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String postalCode2 = Util.getStringValue(r.getAs("postalCode_" + i));
						if (firstNotNull_PostalCode != null && postalCode2 != null && !firstNotNull_PostalCode.equals(postalCode2)) {
							suppliers_res.addLogEvent("Data consistency problem for [Suppliers - id :"+suppliers_res.getSupplierId()+"]: different values found for attribute 'Suppliers.postalCode': " + firstNotNull_PostalCode + " and " + postalCode2 + "." );
							logger.warn("Data consistency problem for [Suppliers - id :"+suppliers_res.getSupplierId()+"]: different values found for attribute 'Suppliers.postalCode': " + firstNotNull_PostalCode + " and " + postalCode2 + "." );
						}
						if (firstNotNull_PostalCode == null && postalCode2 != null) {
							firstNotNull_PostalCode = postalCode2;
						}
					}
					suppliers_res.setPostalCode(firstNotNull_PostalCode);
					
					// attribute 'Suppliers.country'
					String firstNotNull_Country = Util.getStringValue(r.getAs("country"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String country2 = Util.getStringValue(r.getAs("country_" + i));
						if (firstNotNull_Country != null && country2 != null && !firstNotNull_Country.equals(country2)) {
							suppliers_res.addLogEvent("Data consistency problem for [Suppliers - id :"+suppliers_res.getSupplierId()+"]: different values found for attribute 'Suppliers.country': " + firstNotNull_Country + " and " + country2 + "." );
							logger.warn("Data consistency problem for [Suppliers - id :"+suppliers_res.getSupplierId()+"]: different values found for attribute 'Suppliers.country': " + firstNotNull_Country + " and " + country2 + "." );
						}
						if (firstNotNull_Country == null && country2 != null) {
							firstNotNull_Country = country2;
						}
					}
					suppliers_res.setCountry(firstNotNull_Country);
					
					// attribute 'Suppliers.phone'
					String firstNotNull_Phone = Util.getStringValue(r.getAs("phone"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String phone2 = Util.getStringValue(r.getAs("phone_" + i));
						if (firstNotNull_Phone != null && phone2 != null && !firstNotNull_Phone.equals(phone2)) {
							suppliers_res.addLogEvent("Data consistency problem for [Suppliers - id :"+suppliers_res.getSupplierId()+"]: different values found for attribute 'Suppliers.phone': " + firstNotNull_Phone + " and " + phone2 + "." );
							logger.warn("Data consistency problem for [Suppliers - id :"+suppliers_res.getSupplierId()+"]: different values found for attribute 'Suppliers.phone': " + firstNotNull_Phone + " and " + phone2 + "." );
						}
						if (firstNotNull_Phone == null && phone2 != null) {
							firstNotNull_Phone = phone2;
						}
					}
					suppliers_res.setPhone(firstNotNull_Phone);
					
					// attribute 'Suppliers.fax'
					String firstNotNull_Fax = Util.getStringValue(r.getAs("fax"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String fax2 = Util.getStringValue(r.getAs("fax_" + i));
						if (firstNotNull_Fax != null && fax2 != null && !firstNotNull_Fax.equals(fax2)) {
							suppliers_res.addLogEvent("Data consistency problem for [Suppliers - id :"+suppliers_res.getSupplierId()+"]: different values found for attribute 'Suppliers.fax': " + firstNotNull_Fax + " and " + fax2 + "." );
							logger.warn("Data consistency problem for [Suppliers - id :"+suppliers_res.getSupplierId()+"]: different values found for attribute 'Suppliers.fax': " + firstNotNull_Fax + " and " + fax2 + "." );
						}
						if (firstNotNull_Fax == null && fax2 != null) {
							firstNotNull_Fax = fax2;
						}
					}
					suppliers_res.setFax(firstNotNull_Fax);
					
					// attribute 'Suppliers.homePage'
					String firstNotNull_HomePage = Util.getStringValue(r.getAs("homePage"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String homePage2 = Util.getStringValue(r.getAs("homePage_" + i));
						if (firstNotNull_HomePage != null && homePage2 != null && !firstNotNull_HomePage.equals(homePage2)) {
							suppliers_res.addLogEvent("Data consistency problem for [Suppliers - id :"+suppliers_res.getSupplierId()+"]: different values found for attribute 'Suppliers.homePage': " + firstNotNull_HomePage + " and " + homePage2 + "." );
							logger.warn("Data consistency problem for [Suppliers - id :"+suppliers_res.getSupplierId()+"]: different values found for attribute 'Suppliers.homePage': " + firstNotNull_HomePage + " and " + homePage2 + "." );
						}
						if (firstNotNull_HomePage == null && homePage2 != null) {
							firstNotNull_HomePage = homePage2;
						}
					}
					suppliers_res.setHomePage(firstNotNull_HomePage);
	
					WrappedArray logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							suppliers_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							suppliers_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					return suppliers_res;
				}, Encoders.bean(Suppliers.class));
			return d;
	}
	
	
	
	
	
	public Suppliers getSuppliers(Suppliers.supply role, Products products) {
		if(role != null) {
			if(role.equals(Suppliers.supply.supplier))
				return getSupplierInSupplyBySuppliedProduct(products);
		}
		return null;
	}
	
	public Dataset<Suppliers> getSuppliersList(Suppliers.supply role, Condition<ProductsAttribute> condition) {
		if(role != null) {
			if(role.equals(Suppliers.supply.supplier))
				return getSupplierListInSupplyBySuppliedProductCondition(condition);
		}
		return null;
	}
	
	public Dataset<Suppliers> getSuppliersList(Suppliers.supply role, Condition<ProductsAttribute> condition1, Condition<SuppliersAttribute> condition2) {
		if(role != null) {
			if(role.equals(Suppliers.supply.supplier))
				return getSupplierListInSupply(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	
	
	
	
	public abstract Dataset<Suppliers> getSupplierListInSupply(conditions.Condition<conditions.ProductsAttribute> suppliedProduct_condition,conditions.Condition<conditions.SuppliersAttribute> supplier_condition);
	
	public Dataset<Suppliers> getSupplierListInSupplyBySuppliedProductCondition(conditions.Condition<conditions.ProductsAttribute> suppliedProduct_condition){
		return getSupplierListInSupply(suppliedProduct_condition, null);
	}
	
	public Suppliers getSupplierInSupplyBySuppliedProduct(pojo.Products suppliedProduct){
		if(suppliedProduct == null)
			return null;
	
		Condition c;
		c=Condition.simple(ProductsAttribute.productId,Operator.EQUALS, suppliedProduct.getProductId());
		Dataset<Suppliers> res = getSupplierListInSupplyBySuppliedProductCondition(c);
		return !res.isEmpty()?res.first():null;
	}
	
	public Dataset<Suppliers> getSupplierListInSupplyBySupplierCondition(conditions.Condition<conditions.SuppliersAttribute> supplier_condition){
		return getSupplierListInSupply(null, supplier_condition);
	}
	
	
	public abstract boolean insertSuppliers(Suppliers suppliers);
	
	public abstract boolean insertSuppliersInSuppliersFromMyRedisDB(Suppliers suppliers); 
	private boolean inUpdateMethod = false;
	private List<Row> allSuppliersIdList = null;
	public abstract void updateSuppliersList(conditions.Condition<conditions.SuppliersAttribute> condition, conditions.SetClause<conditions.SuppliersAttribute> set);
	
	public void updateSuppliers(pojo.Suppliers suppliers) {
		//TODO using the id
		return;
	}
	public abstract void updateSupplierListInSupply(
		conditions.Condition<conditions.ProductsAttribute> suppliedProduct_condition,
		conditions.Condition<conditions.SuppliersAttribute> supplier_condition,
		
		conditions.SetClause<conditions.SuppliersAttribute> set
	);
	
	public void updateSupplierListInSupplyBySuppliedProductCondition(
		conditions.Condition<conditions.ProductsAttribute> suppliedProduct_condition,
		conditions.SetClause<conditions.SuppliersAttribute> set
	){
		updateSupplierListInSupply(suppliedProduct_condition, null, set);
	}
	
	public void updateSupplierInSupplyBySuppliedProduct(
		pojo.Products suppliedProduct,
		conditions.SetClause<conditions.SuppliersAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateSupplierListInSupplyBySupplierCondition(
		conditions.Condition<conditions.SuppliersAttribute> supplier_condition,
		conditions.SetClause<conditions.SuppliersAttribute> set
	){
		updateSupplierListInSupply(null, supplier_condition, set);
	}
	
	
	public abstract void deleteSuppliersList(conditions.Condition<conditions.SuppliersAttribute> condition);
	
	public void deleteSuppliers(pojo.Suppliers suppliers) {
		//TODO using the id
		return;
	}
	public abstract void deleteSupplierListInSupply(	
		conditions.Condition<conditions.ProductsAttribute> suppliedProduct_condition,	
		conditions.Condition<conditions.SuppliersAttribute> supplier_condition);
	
	public void deleteSupplierListInSupplyBySuppliedProductCondition(
		conditions.Condition<conditions.ProductsAttribute> suppliedProduct_condition
	){
		deleteSupplierListInSupply(suppliedProduct_condition, null);
	}
	
	public void deleteSupplierInSupplyBySuppliedProduct(
		pojo.Products suppliedProduct 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteSupplierListInSupplyBySupplierCondition(
		conditions.Condition<conditions.SuppliersAttribute> supplier_condition
	){
		deleteSupplierListInSupply(null, supplier_condition);
	}
	
}
