package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import pojo.Employees;
import conditions.EmployeesAttribute;
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
import conditions.EmployeesAttribute;
import pojo.Works;
import conditions.TerritoriesAttribute;
import pojo.Territories;
import conditions.EmployeesAttribute;
import pojo.ReportsTo;
import conditions.EmployeesAttribute;
import pojo.Employees;
import conditions.EmployeesAttribute;
import pojo.ReportsTo;
import conditions.EmployeesAttribute;
import pojo.Employees;
import conditions.EmployeesAttribute;
import pojo.Register;
import conditions.OrdersAttribute;
import pojo.Orders;

public abstract class EmployeesService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EmployeesService.class);
	protected WorksService worksService = new dao.impl.WorksServiceImpl();
	protected ReportsToService reportsToService = new dao.impl.ReportsToServiceImpl();
	protected RegisterService registerService = new dao.impl.RegisterServiceImpl();
	


	public static enum ROLE_NAME {
		WORKS_EMPLOYED, REPORTSTO_SUBORDONEE, REPORTSTO_BOSS, REGISTER_EMPLOYEEINCHARGE
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.WORKS_EMPLOYED, loading.Loading.LAZY);
		defaultLoadingParameters.put(ROLE_NAME.REPORTSTO_SUBORDONEE, loading.Loading.EAGER);
		defaultLoadingParameters.put(ROLE_NAME.REPORTSTO_BOSS, loading.Loading.LAZY);
		defaultLoadingParameters.put(ROLE_NAME.REGISTER_EMPLOYEEINCHARGE, loading.Loading.LAZY);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public EmployeesService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public EmployeesService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
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
	
	
	public Dataset<Employees> getEmployeesList(){
		return getEmployeesList(null);
	}
	
	public Dataset<Employees> getEmployeesList(conditions.Condition<conditions.EmployeesAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Employees>> datasets = new ArrayList<Dataset<Employees>>();
		Dataset<Employees> d = null;
		d = getEmployeesListInEmployeesFromMyRelDB(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsEmployees(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Employees>) r -> condition == null || condition.evaluate(r));
		d = d.dropDuplicates(new String[] {"employeeID"});
		return d;
	}
	
	
	
	
	
	public abstract Dataset<Employees> getEmployeesListInEmployeesFromMyRelDB(conditions.Condition<conditions.EmployeesAttribute> condition, MutableBoolean refilterFlag);
	
	
	public Employees getEmployeesById(Integer employeeID){
		Condition cond;
		cond = Condition.simple(EmployeesAttribute.employeeID, conditions.Operator.EQUALS, employeeID);
		Dataset<Employees> res = getEmployeesList(cond);
		if(res!=null && !res.isEmpty())
			return res.first();
		return null;
	}
	
	public Dataset<Employees> getEmployeesListByEmployeeID(Integer employeeID) {
		return getEmployeesList(conditions.Condition.simple(conditions.EmployeesAttribute.employeeID, conditions.Operator.EQUALS, employeeID));
	}
	
	public Dataset<Employees> getEmployeesListByLastName(String lastName) {
		return getEmployeesList(conditions.Condition.simple(conditions.EmployeesAttribute.lastName, conditions.Operator.EQUALS, lastName));
	}
	
	public Dataset<Employees> getEmployeesListByFirstName(String firstName) {
		return getEmployeesList(conditions.Condition.simple(conditions.EmployeesAttribute.firstName, conditions.Operator.EQUALS, firstName));
	}
	
	public Dataset<Employees> getEmployeesListByTitle(String title) {
		return getEmployeesList(conditions.Condition.simple(conditions.EmployeesAttribute.title, conditions.Operator.EQUALS, title));
	}
	
	public Dataset<Employees> getEmployeesListByTitleOfCourtesy(String titleOfCourtesy) {
		return getEmployeesList(conditions.Condition.simple(conditions.EmployeesAttribute.titleOfCourtesy, conditions.Operator.EQUALS, titleOfCourtesy));
	}
	
	public Dataset<Employees> getEmployeesListByBirthDate(LocalDate birthDate) {
		return getEmployeesList(conditions.Condition.simple(conditions.EmployeesAttribute.birthDate, conditions.Operator.EQUALS, birthDate));
	}
	
	public Dataset<Employees> getEmployeesListByHireDate(LocalDate hireDate) {
		return getEmployeesList(conditions.Condition.simple(conditions.EmployeesAttribute.hireDate, conditions.Operator.EQUALS, hireDate));
	}
	
	public Dataset<Employees> getEmployeesListByAddress(String address) {
		return getEmployeesList(conditions.Condition.simple(conditions.EmployeesAttribute.address, conditions.Operator.EQUALS, address));
	}
	
	public Dataset<Employees> getEmployeesListByCity(String city) {
		return getEmployeesList(conditions.Condition.simple(conditions.EmployeesAttribute.city, conditions.Operator.EQUALS, city));
	}
	
	public Dataset<Employees> getEmployeesListByRegion(String region) {
		return getEmployeesList(conditions.Condition.simple(conditions.EmployeesAttribute.region, conditions.Operator.EQUALS, region));
	}
	
	public Dataset<Employees> getEmployeesListByPostalCode(String postalCode) {
		return getEmployeesList(conditions.Condition.simple(conditions.EmployeesAttribute.postalCode, conditions.Operator.EQUALS, postalCode));
	}
	
	public Dataset<Employees> getEmployeesListByCountry(String country) {
		return getEmployeesList(conditions.Condition.simple(conditions.EmployeesAttribute.country, conditions.Operator.EQUALS, country));
	}
	
	public Dataset<Employees> getEmployeesListByHomePhone(String homePhone) {
		return getEmployeesList(conditions.Condition.simple(conditions.EmployeesAttribute.homePhone, conditions.Operator.EQUALS, homePhone));
	}
	
	public Dataset<Employees> getEmployeesListByExtension(String extension) {
		return getEmployeesList(conditions.Condition.simple(conditions.EmployeesAttribute.extension, conditions.Operator.EQUALS, extension));
	}
	
	public Dataset<Employees> getEmployeesListByPhoto(byte[] photo) {
		return getEmployeesList(conditions.Condition.simple(conditions.EmployeesAttribute.photo, conditions.Operator.EQUALS, photo));
	}
	
	public Dataset<Employees> getEmployeesListByNotes(String notes) {
		return getEmployeesList(conditions.Condition.simple(conditions.EmployeesAttribute.notes, conditions.Operator.EQUALS, notes));
	}
	
	public Dataset<Employees> getEmployeesListByPhotoPath(String photoPath) {
		return getEmployeesList(conditions.Condition.simple(conditions.EmployeesAttribute.photoPath, conditions.Operator.EQUALS, photoPath));
	}
	
	public Dataset<Employees> getEmployeesListBySalary(Double salary) {
		return getEmployeesList(conditions.Condition.simple(conditions.EmployeesAttribute.salary, conditions.Operator.EQUALS, salary));
	}
	
	
	
	public static Dataset<Employees> fullOuterJoinsEmployees(List<Dataset<Employees>> datasetsPOJO) {
		return fullOuterJoinsEmployees(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Employees> fullLeftOuterJoinsEmployees(List<Dataset<Employees>> datasetsPOJO) {
		return fullOuterJoinsEmployees(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Employees> fullOuterJoinsEmployees(List<Dataset<Employees>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Employees> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("employeeID");
			logger.debug("Start {} of [{}] datasets of [Employees] objects",joinMode,datasetsPOJO.size());
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("lastName", "lastName_1")
								.withColumnRenamed("firstName", "firstName_1")
								.withColumnRenamed("title", "title_1")
								.withColumnRenamed("titleOfCourtesy", "titleOfCourtesy_1")
								.withColumnRenamed("birthDate", "birthDate_1")
								.withColumnRenamed("hireDate", "hireDate_1")
								.withColumnRenamed("address", "address_1")
								.withColumnRenamed("city", "city_1")
								.withColumnRenamed("region", "region_1")
								.withColumnRenamed("postalCode", "postalCode_1")
								.withColumnRenamed("country", "country_1")
								.withColumnRenamed("homePhone", "homePhone_1")
								.withColumnRenamed("extension", "extension_1")
								.withColumnRenamed("photo", "photo_1")
								.withColumnRenamed("notes", "notes_1")
								.withColumnRenamed("photoPath", "photoPath_1")
								.withColumnRenamed("salary", "salary_1")
								.withColumnRenamed("logEvents", "logEvents_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("lastName", "lastName_" + i)
								.withColumnRenamed("firstName", "firstName_" + i)
								.withColumnRenamed("title", "title_" + i)
								.withColumnRenamed("titleOfCourtesy", "titleOfCourtesy_" + i)
								.withColumnRenamed("birthDate", "birthDate_" + i)
								.withColumnRenamed("hireDate", "hireDate_" + i)
								.withColumnRenamed("address", "address_" + i)
								.withColumnRenamed("city", "city_" + i)
								.withColumnRenamed("region", "region_" + i)
								.withColumnRenamed("postalCode", "postalCode_" + i)
								.withColumnRenamed("country", "country_" + i)
								.withColumnRenamed("homePhone", "homePhone_" + i)
								.withColumnRenamed("extension", "extension_" + i)
								.withColumnRenamed("photo", "photo_" + i)
								.withColumnRenamed("notes", "notes_" + i)
								.withColumnRenamed("photoPath", "photoPath_" + i)
								.withColumnRenamed("salary", "salary_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
						, seq, joinMode);
			}
			logger.debug("End join. Start");
			logger.debug("Start transforming Row objects to [Employees] objects"); 
			d = res.map((MapFunction<Row, Employees>) r -> {
					Employees employees_res = new Employees();
					
					// attribute 'Employees.employeeID'
					Integer firstNotNull_employeeID = Util.getIntegerValue(r.getAs("employeeID"));
					employees_res.setEmployeeID(firstNotNull_employeeID);
					
					// attribute 'Employees.lastName'
					String firstNotNull_lastName = Util.getStringValue(r.getAs("lastName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String lastName2 = Util.getStringValue(r.getAs("lastName_" + i));
						if (firstNotNull_lastName != null && lastName2 != null && !firstNotNull_lastName.equals(lastName2)) {
							employees_res.addLogEvent("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.lastName': " + firstNotNull_lastName + " and " + lastName2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.lastName': " + firstNotNull_lastName + " and " + lastName2 + "." );
						}
						if (firstNotNull_lastName == null && lastName2 != null) {
							firstNotNull_lastName = lastName2;
						}
					}
					employees_res.setLastName(firstNotNull_lastName);
					
					// attribute 'Employees.firstName'
					String firstNotNull_firstName = Util.getStringValue(r.getAs("firstName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String firstName2 = Util.getStringValue(r.getAs("firstName_" + i));
						if (firstNotNull_firstName != null && firstName2 != null && !firstNotNull_firstName.equals(firstName2)) {
							employees_res.addLogEvent("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.firstName': " + firstNotNull_firstName + " and " + firstName2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.firstName': " + firstNotNull_firstName + " and " + firstName2 + "." );
						}
						if (firstNotNull_firstName == null && firstName2 != null) {
							firstNotNull_firstName = firstName2;
						}
					}
					employees_res.setFirstName(firstNotNull_firstName);
					
					// attribute 'Employees.title'
					String firstNotNull_title = Util.getStringValue(r.getAs("title"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String title2 = Util.getStringValue(r.getAs("title_" + i));
						if (firstNotNull_title != null && title2 != null && !firstNotNull_title.equals(title2)) {
							employees_res.addLogEvent("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.title': " + firstNotNull_title + " and " + title2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.title': " + firstNotNull_title + " and " + title2 + "." );
						}
						if (firstNotNull_title == null && title2 != null) {
							firstNotNull_title = title2;
						}
					}
					employees_res.setTitle(firstNotNull_title);
					
					// attribute 'Employees.titleOfCourtesy'
					String firstNotNull_titleOfCourtesy = Util.getStringValue(r.getAs("titleOfCourtesy"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String titleOfCourtesy2 = Util.getStringValue(r.getAs("titleOfCourtesy_" + i));
						if (firstNotNull_titleOfCourtesy != null && titleOfCourtesy2 != null && !firstNotNull_titleOfCourtesy.equals(titleOfCourtesy2)) {
							employees_res.addLogEvent("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.titleOfCourtesy': " + firstNotNull_titleOfCourtesy + " and " + titleOfCourtesy2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.titleOfCourtesy': " + firstNotNull_titleOfCourtesy + " and " + titleOfCourtesy2 + "." );
						}
						if (firstNotNull_titleOfCourtesy == null && titleOfCourtesy2 != null) {
							firstNotNull_titleOfCourtesy = titleOfCourtesy2;
						}
					}
					employees_res.setTitleOfCourtesy(firstNotNull_titleOfCourtesy);
					
					// attribute 'Employees.birthDate'
					LocalDate firstNotNull_birthDate = Util.getLocalDateValue(r.getAs("birthDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate birthDate2 = Util.getLocalDateValue(r.getAs("birthDate_" + i));
						if (firstNotNull_birthDate != null && birthDate2 != null && !firstNotNull_birthDate.equals(birthDate2)) {
							employees_res.addLogEvent("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.birthDate': " + firstNotNull_birthDate + " and " + birthDate2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.birthDate': " + firstNotNull_birthDate + " and " + birthDate2 + "." );
						}
						if (firstNotNull_birthDate == null && birthDate2 != null) {
							firstNotNull_birthDate = birthDate2;
						}
					}
					employees_res.setBirthDate(firstNotNull_birthDate);
					
					// attribute 'Employees.hireDate'
					LocalDate firstNotNull_hireDate = Util.getLocalDateValue(r.getAs("hireDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate hireDate2 = Util.getLocalDateValue(r.getAs("hireDate_" + i));
						if (firstNotNull_hireDate != null && hireDate2 != null && !firstNotNull_hireDate.equals(hireDate2)) {
							employees_res.addLogEvent("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.hireDate': " + firstNotNull_hireDate + " and " + hireDate2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.hireDate': " + firstNotNull_hireDate + " and " + hireDate2 + "." );
						}
						if (firstNotNull_hireDate == null && hireDate2 != null) {
							firstNotNull_hireDate = hireDate2;
						}
					}
					employees_res.setHireDate(firstNotNull_hireDate);
					
					// attribute 'Employees.address'
					String firstNotNull_address = Util.getStringValue(r.getAs("address"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String address2 = Util.getStringValue(r.getAs("address_" + i));
						if (firstNotNull_address != null && address2 != null && !firstNotNull_address.equals(address2)) {
							employees_res.addLogEvent("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.address': " + firstNotNull_address + " and " + address2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.address': " + firstNotNull_address + " and " + address2 + "." );
						}
						if (firstNotNull_address == null && address2 != null) {
							firstNotNull_address = address2;
						}
					}
					employees_res.setAddress(firstNotNull_address);
					
					// attribute 'Employees.city'
					String firstNotNull_city = Util.getStringValue(r.getAs("city"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String city2 = Util.getStringValue(r.getAs("city_" + i));
						if (firstNotNull_city != null && city2 != null && !firstNotNull_city.equals(city2)) {
							employees_res.addLogEvent("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.city': " + firstNotNull_city + " and " + city2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.city': " + firstNotNull_city + " and " + city2 + "." );
						}
						if (firstNotNull_city == null && city2 != null) {
							firstNotNull_city = city2;
						}
					}
					employees_res.setCity(firstNotNull_city);
					
					// attribute 'Employees.region'
					String firstNotNull_region = Util.getStringValue(r.getAs("region"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String region2 = Util.getStringValue(r.getAs("region_" + i));
						if (firstNotNull_region != null && region2 != null && !firstNotNull_region.equals(region2)) {
							employees_res.addLogEvent("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.region': " + firstNotNull_region + " and " + region2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.region': " + firstNotNull_region + " and " + region2 + "." );
						}
						if (firstNotNull_region == null && region2 != null) {
							firstNotNull_region = region2;
						}
					}
					employees_res.setRegion(firstNotNull_region);
					
					// attribute 'Employees.postalCode'
					String firstNotNull_postalCode = Util.getStringValue(r.getAs("postalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String postalCode2 = Util.getStringValue(r.getAs("postalCode_" + i));
						if (firstNotNull_postalCode != null && postalCode2 != null && !firstNotNull_postalCode.equals(postalCode2)) {
							employees_res.addLogEvent("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.postalCode': " + firstNotNull_postalCode + " and " + postalCode2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.postalCode': " + firstNotNull_postalCode + " and " + postalCode2 + "." );
						}
						if (firstNotNull_postalCode == null && postalCode2 != null) {
							firstNotNull_postalCode = postalCode2;
						}
					}
					employees_res.setPostalCode(firstNotNull_postalCode);
					
					// attribute 'Employees.country'
					String firstNotNull_country = Util.getStringValue(r.getAs("country"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String country2 = Util.getStringValue(r.getAs("country_" + i));
						if (firstNotNull_country != null && country2 != null && !firstNotNull_country.equals(country2)) {
							employees_res.addLogEvent("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.country': " + firstNotNull_country + " and " + country2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.country': " + firstNotNull_country + " and " + country2 + "." );
						}
						if (firstNotNull_country == null && country2 != null) {
							firstNotNull_country = country2;
						}
					}
					employees_res.setCountry(firstNotNull_country);
					
					// attribute 'Employees.homePhone'
					String firstNotNull_homePhone = Util.getStringValue(r.getAs("homePhone"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String homePhone2 = Util.getStringValue(r.getAs("homePhone_" + i));
						if (firstNotNull_homePhone != null && homePhone2 != null && !firstNotNull_homePhone.equals(homePhone2)) {
							employees_res.addLogEvent("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.homePhone': " + firstNotNull_homePhone + " and " + homePhone2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.homePhone': " + firstNotNull_homePhone + " and " + homePhone2 + "." );
						}
						if (firstNotNull_homePhone == null && homePhone2 != null) {
							firstNotNull_homePhone = homePhone2;
						}
					}
					employees_res.setHomePhone(firstNotNull_homePhone);
					
					// attribute 'Employees.extension'
					String firstNotNull_extension = Util.getStringValue(r.getAs("extension"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String extension2 = Util.getStringValue(r.getAs("extension_" + i));
						if (firstNotNull_extension != null && extension2 != null && !firstNotNull_extension.equals(extension2)) {
							employees_res.addLogEvent("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.extension': " + firstNotNull_extension + " and " + extension2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.extension': " + firstNotNull_extension + " and " + extension2 + "." );
						}
						if (firstNotNull_extension == null && extension2 != null) {
							firstNotNull_extension = extension2;
						}
					}
					employees_res.setExtension(firstNotNull_extension);
					
					// attribute 'Employees.photo'
					byte[] firstNotNull_photo = Util.getByteArrayValue(r.getAs("photo"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						byte[] photo2 = Util.getByteArrayValue(r.getAs("photo_" + i));
						if (firstNotNull_photo != null && photo2 != null && !firstNotNull_photo.equals(photo2)) {
							employees_res.addLogEvent("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.photo': " + firstNotNull_photo + " and " + photo2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.photo': " + firstNotNull_photo + " and " + photo2 + "." );
						}
						if (firstNotNull_photo == null && photo2 != null) {
							firstNotNull_photo = photo2;
						}
					}
					employees_res.setPhoto(firstNotNull_photo);
					
					// attribute 'Employees.notes'
					String firstNotNull_notes = Util.getStringValue(r.getAs("notes"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String notes2 = Util.getStringValue(r.getAs("notes_" + i));
						if (firstNotNull_notes != null && notes2 != null && !firstNotNull_notes.equals(notes2)) {
							employees_res.addLogEvent("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.notes': " + firstNotNull_notes + " and " + notes2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.notes': " + firstNotNull_notes + " and " + notes2 + "." );
						}
						if (firstNotNull_notes == null && notes2 != null) {
							firstNotNull_notes = notes2;
						}
					}
					employees_res.setNotes(firstNotNull_notes);
					
					// attribute 'Employees.photoPath'
					String firstNotNull_photoPath = Util.getStringValue(r.getAs("photoPath"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String photoPath2 = Util.getStringValue(r.getAs("photoPath_" + i));
						if (firstNotNull_photoPath != null && photoPath2 != null && !firstNotNull_photoPath.equals(photoPath2)) {
							employees_res.addLogEvent("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.photoPath': " + firstNotNull_photoPath + " and " + photoPath2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.photoPath': " + firstNotNull_photoPath + " and " + photoPath2 + "." );
						}
						if (firstNotNull_photoPath == null && photoPath2 != null) {
							firstNotNull_photoPath = photoPath2;
						}
					}
					employees_res.setPhotoPath(firstNotNull_photoPath);
					
					// attribute 'Employees.salary'
					Double firstNotNull_salary = Util.getDoubleValue(r.getAs("salary"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double salary2 = Util.getDoubleValue(r.getAs("salary_" + i));
						if (firstNotNull_salary != null && salary2 != null && !firstNotNull_salary.equals(salary2)) {
							employees_res.addLogEvent("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.salary': " + firstNotNull_salary + " and " + salary2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employees_res.getEmployeeID()+"]: different values found for attribute 'Employees.salary': " + firstNotNull_salary + " and " + salary2 + "." );
						}
						if (firstNotNull_salary == null && salary2 != null) {
							firstNotNull_salary = salary2;
						}
					}
					employees_res.setSalary(firstNotNull_salary);
	
					WrappedArray logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							employees_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							employees_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					return employees_res;
				}, Encoders.bean(Employees.class));
			return d;
	}
	
	
	
	public Dataset<Employees> getEmployeesList(Employees.works role, Territories territories) {
		if(role != null) {
			if(role.equals(Employees.works.employed))
				return getEmployedListInWorksByTerritories(territories);
		}
		return null;
	}
	
	public Dataset<Employees> getEmployeesList(Employees.works role, Condition<TerritoriesAttribute> condition) {
		if(role != null) {
			if(role.equals(Employees.works.employed))
				return getEmployedListInWorksByTerritoriesCondition(condition);
		}
		return null;
	}
	
	public Dataset<Employees> getEmployeesList(Employees.works role, Condition<EmployeesAttribute> condition1, Condition<TerritoriesAttribute> condition2) {
		if(role != null) {
			if(role.equals(Employees.works.employed))
				return getEmployedListInWorks(condition1, condition2);
		}
		return null;
	}
	
	
	
	public Dataset<Employees> getEmployeesList(Employees.reportsTo role, Employees employees) {
		if(role != null) {
			if(role.equals(Employees.reportsTo.subordonee))
				return getSubordoneeListInReportsToByBoss(employees);
		}
		return null;
	}
	
	public Employees getEmployees(Employees.reportsTo role, Employees employees) {
		if(role != null) {
			if(role.equals(Employees.reportsTo.boss))
				return getBossInReportsToBySubordonee(employees);
		}
		return null;
	}
	
	public Dataset<Employees> getEmployeesList(Employees.reportsTo role, Condition<EmployeesAttribute> condition) {
		if(role != null) {
			if(role.equals(Employees.reportsTo.subordonee))
				return getSubordoneeListInReportsToByBossCondition(condition);
			if(role.equals(Employees.reportsTo.boss))
				return getBossListInReportsToBySubordoneeCondition(condition);
		}
		return null;
	}
	
	public Dataset<Employees> getEmployeesList(Employees.reportsTo role, Condition<EmployeesAttribute> condition1, Condition<EmployeesAttribute> condition2) {
		if(role != null) {
			if(role.equals(Employees.reportsTo.subordonee))
				return getSubordoneeListInReportsTo(condition1, condition2);
			if(role.equals(Employees.reportsTo.boss))
				return getBossListInReportsTo(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	public Employees getEmployees(Employees.register role, Orders orders) {
		if(role != null) {
			if(role.equals(Employees.register.employeeInCharge))
				return getEmployeeInChargeInRegisterByProcessedOrder(orders);
		}
		return null;
	}
	
	public Dataset<Employees> getEmployeesList(Employees.register role, Condition<OrdersAttribute> condition) {
		if(role != null) {
			if(role.equals(Employees.register.employeeInCharge))
				return getEmployeeInChargeListInRegisterByProcessedOrderCondition(condition);
		}
		return null;
	}
	
	public Dataset<Employees> getEmployeesList(Employees.register role, Condition<OrdersAttribute> condition1, Condition<EmployeesAttribute> condition2) {
		if(role != null) {
			if(role.equals(Employees.register.employeeInCharge))
				return getEmployeeInChargeListInRegister(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	
	public abstract Dataset<Employees> getEmployedListInWorks(conditions.Condition<conditions.EmployeesAttribute> employed_condition,conditions.Condition<conditions.TerritoriesAttribute> territories_condition);
	
	public Dataset<Employees> getEmployedListInWorksByEmployedCondition(conditions.Condition<conditions.EmployeesAttribute> employed_condition){
		return getEmployedListInWorks(employed_condition, null);
	}
	public Dataset<Employees> getEmployedListInWorksByTerritoriesCondition(conditions.Condition<conditions.TerritoriesAttribute> territories_condition){
		return getEmployedListInWorks(null, territories_condition);
	}
	
	public Dataset<Employees> getEmployedListInWorksByTerritories(pojo.Territories territories){
		if(territories == null)
			return null;
	
		Condition c;
		c=Condition.simple(TerritoriesAttribute.territoryID,Operator.EQUALS, territories.getTerritoryID());
		Dataset<Employees> res = getEmployedListInWorksByTerritoriesCondition(c);
		return res;
	}
	
	public abstract Dataset<Employees> getSubordoneeListInReportsTo(conditions.Condition<conditions.EmployeesAttribute> subordonee_condition,conditions.Condition<conditions.EmployeesAttribute> boss_condition);
	
	public Dataset<Employees> getSubordoneeListInReportsToBySubordoneeCondition(conditions.Condition<conditions.EmployeesAttribute> subordonee_condition){
		return getSubordoneeListInReportsTo(subordonee_condition, null);
	}
	public Dataset<Employees> getSubordoneeListInReportsToByBossCondition(conditions.Condition<conditions.EmployeesAttribute> boss_condition){
		return getSubordoneeListInReportsTo(null, boss_condition);
	}
	
	public Dataset<Employees> getSubordoneeListInReportsToByBoss(pojo.Employees boss){
		if(boss == null)
			return null;
	
		Condition c;
		c=Condition.simple(EmployeesAttribute.employeeID,Operator.EQUALS, boss.getEmployeeID());
		Dataset<Employees> res = getSubordoneeListInReportsToByBossCondition(c);
		return res;
	}
	
	public abstract Dataset<Employees> getBossListInReportsTo(conditions.Condition<conditions.EmployeesAttribute> subordonee_condition,conditions.Condition<conditions.EmployeesAttribute> boss_condition);
	
	public Dataset<Employees> getBossListInReportsToBySubordoneeCondition(conditions.Condition<conditions.EmployeesAttribute> subordonee_condition){
		return getBossListInReportsTo(subordonee_condition, null);
	}
	
	public Employees getBossInReportsToBySubordonee(pojo.Employees subordonee){
		if(subordonee == null)
			return null;
	
		Condition c;
		c=Condition.simple(EmployeesAttribute.employeeID,Operator.EQUALS, subordonee.getEmployeeID());
		Dataset<Employees> res = getBossListInReportsToBySubordoneeCondition(c);
		return !res.isEmpty()?res.first():null;
	}
	
	public Dataset<Employees> getBossListInReportsToByBossCondition(conditions.Condition<conditions.EmployeesAttribute> boss_condition){
		return getBossListInReportsTo(null, boss_condition);
	}
	public abstract Dataset<Employees> getEmployeeInChargeListInRegister(conditions.Condition<conditions.OrdersAttribute> processedOrder_condition,conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition);
	
	public Dataset<Employees> getEmployeeInChargeListInRegisterByProcessedOrderCondition(conditions.Condition<conditions.OrdersAttribute> processedOrder_condition){
		return getEmployeeInChargeListInRegister(processedOrder_condition, null);
	}
	
	public Employees getEmployeeInChargeInRegisterByProcessedOrder(pojo.Orders processedOrder){
		if(processedOrder == null)
			return null;
	
		Condition c;
		c=Condition.simple(OrdersAttribute.id,Operator.EQUALS, processedOrder.getId());
		Dataset<Employees> res = getEmployeeInChargeListInRegisterByProcessedOrderCondition(c);
		return !res.isEmpty()?res.first():null;
	}
	
	public Dataset<Employees> getEmployeeInChargeListInRegisterByEmployeeInChargeCondition(conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition){
		return getEmployeeInChargeListInRegister(null, employeeInCharge_condition);
	}
	
	
	public abstract boolean insertEmployees(Employees employees);
	
	public abstract boolean insertEmployeesInEmployeesFromMyRelDB(Employees employees); 
	private boolean inUpdateMethod = false;
	private List<Row> allEmployeesIdList = null;
	public abstract void updateEmployeesList(conditions.Condition<conditions.EmployeesAttribute> condition, conditions.SetClause<conditions.EmployeesAttribute> set);
	
	public void updateEmployees(pojo.Employees employees) {
		//TODO using the id
		return;
	}
	public abstract void updateEmployedListInWorks(
		conditions.Condition<conditions.EmployeesAttribute> employed_condition,
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition,
		
		conditions.SetClause<conditions.EmployeesAttribute> set
	);
	
	public void updateEmployedListInWorksByEmployedCondition(
		conditions.Condition<conditions.EmployeesAttribute> employed_condition,
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		updateEmployedListInWorks(employed_condition, null, set);
	}
	public void updateEmployedListInWorksByTerritoriesCondition(
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition,
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		updateEmployedListInWorks(null, territories_condition, set);
	}
	
	public void updateEmployedListInWorksByTerritories(
		pojo.Territories territories,
		conditions.SetClause<conditions.EmployeesAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public abstract void updateSubordoneeListInReportsTo(
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition,
		conditions.Condition<conditions.EmployeesAttribute> boss_condition,
		
		conditions.SetClause<conditions.EmployeesAttribute> set
	);
	
	public void updateSubordoneeListInReportsToBySubordoneeCondition(
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition,
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		updateSubordoneeListInReportsTo(subordonee_condition, null, set);
	}
	public void updateSubordoneeListInReportsToByBossCondition(
		conditions.Condition<conditions.EmployeesAttribute> boss_condition,
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		updateSubordoneeListInReportsTo(null, boss_condition, set);
	}
	
	public void updateSubordoneeListInReportsToByBoss(
		pojo.Employees boss,
		conditions.SetClause<conditions.EmployeesAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public abstract void updateBossListInReportsTo(
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition,
		conditions.Condition<conditions.EmployeesAttribute> boss_condition,
		
		conditions.SetClause<conditions.EmployeesAttribute> set
	);
	
	public void updateBossListInReportsToBySubordoneeCondition(
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition,
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		updateBossListInReportsTo(subordonee_condition, null, set);
	}
	
	public void updateBossInReportsToBySubordonee(
		pojo.Employees subordonee,
		conditions.SetClause<conditions.EmployeesAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateBossListInReportsToByBossCondition(
		conditions.Condition<conditions.EmployeesAttribute> boss_condition,
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		updateBossListInReportsTo(null, boss_condition, set);
	}
	public abstract void updateEmployeeInChargeListInRegister(
		conditions.Condition<conditions.OrdersAttribute> processedOrder_condition,
		conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition,
		
		conditions.SetClause<conditions.EmployeesAttribute> set
	);
	
	public void updateEmployeeInChargeListInRegisterByProcessedOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> processedOrder_condition,
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		updateEmployeeInChargeListInRegister(processedOrder_condition, null, set);
	}
	
	public void updateEmployeeInChargeInRegisterByProcessedOrder(
		pojo.Orders processedOrder,
		conditions.SetClause<conditions.EmployeesAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateEmployeeInChargeListInRegisterByEmployeeInChargeCondition(
		conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition,
		conditions.SetClause<conditions.EmployeesAttribute> set
	){
		updateEmployeeInChargeListInRegister(null, employeeInCharge_condition, set);
	}
	
	
	public abstract void deleteEmployeesList(conditions.Condition<conditions.EmployeesAttribute> condition);
	
	public void deleteEmployees(pojo.Employees employees) {
		//TODO using the id
		return;
	}
	public abstract void deleteEmployedListInWorks(	
		conditions.Condition<conditions.EmployeesAttribute> employed_condition,	
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition);
	
	public void deleteEmployedListInWorksByEmployedCondition(
		conditions.Condition<conditions.EmployeesAttribute> employed_condition
	){
		deleteEmployedListInWorks(employed_condition, null);
	}
	public void deleteEmployedListInWorksByTerritoriesCondition(
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition
	){
		deleteEmployedListInWorks(null, territories_condition);
	}
	
	public void deleteEmployedListInWorksByTerritories(
		pojo.Territories territories 
	){
		//TODO get id in condition
		return;	
	}
	
	public abstract void deleteSubordoneeListInReportsTo(	
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition,	
		conditions.Condition<conditions.EmployeesAttribute> boss_condition);
	
	public void deleteSubordoneeListInReportsToBySubordoneeCondition(
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition
	){
		deleteSubordoneeListInReportsTo(subordonee_condition, null);
	}
	public void deleteSubordoneeListInReportsToByBossCondition(
		conditions.Condition<conditions.EmployeesAttribute> boss_condition
	){
		deleteSubordoneeListInReportsTo(null, boss_condition);
	}
	
	public void deleteSubordoneeListInReportsToByBoss(
		pojo.Employees boss 
	){
		//TODO get id in condition
		return;	
	}
	
	public abstract void deleteBossListInReportsTo(	
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition,	
		conditions.Condition<conditions.EmployeesAttribute> boss_condition);
	
	public void deleteBossListInReportsToBySubordoneeCondition(
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition
	){
		deleteBossListInReportsTo(subordonee_condition, null);
	}
	
	public void deleteBossInReportsToBySubordonee(
		pojo.Employees subordonee 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteBossListInReportsToByBossCondition(
		conditions.Condition<conditions.EmployeesAttribute> boss_condition
	){
		deleteBossListInReportsTo(null, boss_condition);
	}
	public abstract void deleteEmployeeInChargeListInRegister(	
		conditions.Condition<conditions.OrdersAttribute> processedOrder_condition,	
		conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition);
	
	public void deleteEmployeeInChargeListInRegisterByProcessedOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> processedOrder_condition
	){
		deleteEmployeeInChargeListInRegister(processedOrder_condition, null);
	}
	
	public void deleteEmployeeInChargeInRegisterByProcessedOrder(
		pojo.Orders processedOrder 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteEmployeeInChargeListInRegisterByEmployeeInChargeCondition(
		conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition
	){
		deleteEmployeeInChargeListInRegister(null, employeeInCharge_condition);
	}
	
}
