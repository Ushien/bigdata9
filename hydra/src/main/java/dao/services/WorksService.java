package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import conditions.*;
import pojo.Works;
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


public abstract class WorksService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WorksService.class);
	// A<-AB->B . getAListInREL
	
	public abstract Dataset<Works> getWorksListInEmployeesAndEmployeeTerritoriesAndTerritoriesFrommyRelDB(Condition<EmployeesAttribute> employed_cond, Condition<TerritoriesAttribute> territories_cond, MutableBoolean employed_refilter, MutableBoolean territories_refilter);
	
	
	// A<-AB->B . getBListInREL
	
	public abstract Dataset<Works> getWorksListInTerritoriesAndEmployeeTerritoriesAndEmployeesFrommyRelDB(Condition<TerritoriesAttribute> territories_cond, Condition<EmployeesAttribute> employed_cond, MutableBoolean territories_refilter, MutableBoolean employed_refilter);
	
	
	
	
	
	public static Dataset<Works> fullLeftOuterJoinBetweenWorksAndEmployed(Dataset<Works> d1, Dataset<Employees> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("employeeID", "A_employeeID")
			.withColumnRenamed("lastName", "A_lastName")
			.withColumnRenamed("firstName", "A_firstName")
			.withColumnRenamed("title", "A_title")
			.withColumnRenamed("titleOfCourtesy", "A_titleOfCourtesy")
			.withColumnRenamed("birthDate", "A_birthDate")
			.withColumnRenamed("hireDate", "A_hireDate")
			.withColumnRenamed("address", "A_address")
			.withColumnRenamed("city", "A_city")
			.withColumnRenamed("region", "A_region")
			.withColumnRenamed("postalCode", "A_postalCode")
			.withColumnRenamed("country", "A_country")
			.withColumnRenamed("homePhone", "A_homePhone")
			.withColumnRenamed("extension", "A_extension")
			.withColumnRenamed("photo", "A_photo")
			.withColumnRenamed("notes", "A_notes")
			.withColumnRenamed("photoPath", "A_photoPath")
			.withColumnRenamed("salary", "A_salary")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("employed.employeeID").equalTo(d2_.col("A_employeeID"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Works>) r -> {
				Works res = new Works();
	
				Employees employed = new Employees();
				Object o = r.getAs("employed");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						employed.setEmployeeID(Util.getIntegerValue(r2.getAs("employeeID")));
						employed.setLastName(Util.getStringValue(r2.getAs("lastName")));
						employed.setFirstName(Util.getStringValue(r2.getAs("firstName")));
						employed.setTitle(Util.getStringValue(r2.getAs("title")));
						employed.setTitleOfCourtesy(Util.getStringValue(r2.getAs("titleOfCourtesy")));
						employed.setBirthDate(Util.getLocalDateValue(r2.getAs("birthDate")));
						employed.setHireDate(Util.getLocalDateValue(r2.getAs("hireDate")));
						employed.setAddress(Util.getStringValue(r2.getAs("address")));
						employed.setCity(Util.getStringValue(r2.getAs("city")));
						employed.setRegion(Util.getStringValue(r2.getAs("region")));
						employed.setPostalCode(Util.getStringValue(r2.getAs("postalCode")));
						employed.setCountry(Util.getStringValue(r2.getAs("country")));
						employed.setHomePhone(Util.getStringValue(r2.getAs("homePhone")));
						employed.setExtension(Util.getStringValue(r2.getAs("extension")));
						employed.setPhoto(Util.getByteArrayValue(r2.getAs("photo")));
						employed.setNotes(Util.getStringValue(r2.getAs("notes")));
						employed.setPhotoPath(Util.getStringValue(r2.getAs("photoPath")));
						employed.setSalary(Util.getDoubleValue(r2.getAs("salary")));
					} 
					if(o instanceof Employees) {
						employed = (Employees) o;
					}
				}
	
				res.setEmployed(employed);
	
				Integer employeeID = Util.getIntegerValue(r.getAs("A_employeeID"));
				if (employed.getEmployeeID() != null && employeeID != null && !employed.getEmployeeID().equals(employeeID)) {
					res.addLogEvent("Data consistency problem for [Works - different values found for attribute 'Works.employeeID': " + employed.getEmployeeID() + " and " + employeeID + "." );
					logger.warn("Data consistency problem for [Works - different values found for attribute 'Works.employeeID': " + employed.getEmployeeID() + " and " + employeeID + "." );
				}
				if(employeeID != null)
					employed.setEmployeeID(employeeID);
				String lastName = Util.getStringValue(r.getAs("A_lastName"));
				if (employed.getLastName() != null && lastName != null && !employed.getLastName().equals(lastName)) {
					res.addLogEvent("Data consistency problem for [Works - different values found for attribute 'Works.lastName': " + employed.getLastName() + " and " + lastName + "." );
					logger.warn("Data consistency problem for [Works - different values found for attribute 'Works.lastName': " + employed.getLastName() + " and " + lastName + "." );
				}
				if(lastName != null)
					employed.setLastName(lastName);
				String firstName = Util.getStringValue(r.getAs("A_firstName"));
				if (employed.getFirstName() != null && firstName != null && !employed.getFirstName().equals(firstName)) {
					res.addLogEvent("Data consistency problem for [Works - different values found for attribute 'Works.firstName': " + employed.getFirstName() + " and " + firstName + "." );
					logger.warn("Data consistency problem for [Works - different values found for attribute 'Works.firstName': " + employed.getFirstName() + " and " + firstName + "." );
				}
				if(firstName != null)
					employed.setFirstName(firstName);
				String title = Util.getStringValue(r.getAs("A_title"));
				if (employed.getTitle() != null && title != null && !employed.getTitle().equals(title)) {
					res.addLogEvent("Data consistency problem for [Works - different values found for attribute 'Works.title': " + employed.getTitle() + " and " + title + "." );
					logger.warn("Data consistency problem for [Works - different values found for attribute 'Works.title': " + employed.getTitle() + " and " + title + "." );
				}
				if(title != null)
					employed.setTitle(title);
				String titleOfCourtesy = Util.getStringValue(r.getAs("A_titleOfCourtesy"));
				if (employed.getTitleOfCourtesy() != null && titleOfCourtesy != null && !employed.getTitleOfCourtesy().equals(titleOfCourtesy)) {
					res.addLogEvent("Data consistency problem for [Works - different values found for attribute 'Works.titleOfCourtesy': " + employed.getTitleOfCourtesy() + " and " + titleOfCourtesy + "." );
					logger.warn("Data consistency problem for [Works - different values found for attribute 'Works.titleOfCourtesy': " + employed.getTitleOfCourtesy() + " and " + titleOfCourtesy + "." );
				}
				if(titleOfCourtesy != null)
					employed.setTitleOfCourtesy(titleOfCourtesy);
				LocalDate birthDate = Util.getLocalDateValue(r.getAs("A_birthDate"));
				if (employed.getBirthDate() != null && birthDate != null && !employed.getBirthDate().equals(birthDate)) {
					res.addLogEvent("Data consistency problem for [Works - different values found for attribute 'Works.birthDate': " + employed.getBirthDate() + " and " + birthDate + "." );
					logger.warn("Data consistency problem for [Works - different values found for attribute 'Works.birthDate': " + employed.getBirthDate() + " and " + birthDate + "." );
				}
				if(birthDate != null)
					employed.setBirthDate(birthDate);
				LocalDate hireDate = Util.getLocalDateValue(r.getAs("A_hireDate"));
				if (employed.getHireDate() != null && hireDate != null && !employed.getHireDate().equals(hireDate)) {
					res.addLogEvent("Data consistency problem for [Works - different values found for attribute 'Works.hireDate': " + employed.getHireDate() + " and " + hireDate + "." );
					logger.warn("Data consistency problem for [Works - different values found for attribute 'Works.hireDate': " + employed.getHireDate() + " and " + hireDate + "." );
				}
				if(hireDate != null)
					employed.setHireDate(hireDate);
				String address = Util.getStringValue(r.getAs("A_address"));
				if (employed.getAddress() != null && address != null && !employed.getAddress().equals(address)) {
					res.addLogEvent("Data consistency problem for [Works - different values found for attribute 'Works.address': " + employed.getAddress() + " and " + address + "." );
					logger.warn("Data consistency problem for [Works - different values found for attribute 'Works.address': " + employed.getAddress() + " and " + address + "." );
				}
				if(address != null)
					employed.setAddress(address);
				String city = Util.getStringValue(r.getAs("A_city"));
				if (employed.getCity() != null && city != null && !employed.getCity().equals(city)) {
					res.addLogEvent("Data consistency problem for [Works - different values found for attribute 'Works.city': " + employed.getCity() + " and " + city + "." );
					logger.warn("Data consistency problem for [Works - different values found for attribute 'Works.city': " + employed.getCity() + " and " + city + "." );
				}
				if(city != null)
					employed.setCity(city);
				String region = Util.getStringValue(r.getAs("A_region"));
				if (employed.getRegion() != null && region != null && !employed.getRegion().equals(region)) {
					res.addLogEvent("Data consistency problem for [Works - different values found for attribute 'Works.region': " + employed.getRegion() + " and " + region + "." );
					logger.warn("Data consistency problem for [Works - different values found for attribute 'Works.region': " + employed.getRegion() + " and " + region + "." );
				}
				if(region != null)
					employed.setRegion(region);
				String postalCode = Util.getStringValue(r.getAs("A_postalCode"));
				if (employed.getPostalCode() != null && postalCode != null && !employed.getPostalCode().equals(postalCode)) {
					res.addLogEvent("Data consistency problem for [Works - different values found for attribute 'Works.postalCode': " + employed.getPostalCode() + " and " + postalCode + "." );
					logger.warn("Data consistency problem for [Works - different values found for attribute 'Works.postalCode': " + employed.getPostalCode() + " and " + postalCode + "." );
				}
				if(postalCode != null)
					employed.setPostalCode(postalCode);
				String country = Util.getStringValue(r.getAs("A_country"));
				if (employed.getCountry() != null && country != null && !employed.getCountry().equals(country)) {
					res.addLogEvent("Data consistency problem for [Works - different values found for attribute 'Works.country': " + employed.getCountry() + " and " + country + "." );
					logger.warn("Data consistency problem for [Works - different values found for attribute 'Works.country': " + employed.getCountry() + " and " + country + "." );
				}
				if(country != null)
					employed.setCountry(country);
				String homePhone = Util.getStringValue(r.getAs("A_homePhone"));
				if (employed.getHomePhone() != null && homePhone != null && !employed.getHomePhone().equals(homePhone)) {
					res.addLogEvent("Data consistency problem for [Works - different values found for attribute 'Works.homePhone': " + employed.getHomePhone() + " and " + homePhone + "." );
					logger.warn("Data consistency problem for [Works - different values found for attribute 'Works.homePhone': " + employed.getHomePhone() + " and " + homePhone + "." );
				}
				if(homePhone != null)
					employed.setHomePhone(homePhone);
				String extension = Util.getStringValue(r.getAs("A_extension"));
				if (employed.getExtension() != null && extension != null && !employed.getExtension().equals(extension)) {
					res.addLogEvent("Data consistency problem for [Works - different values found for attribute 'Works.extension': " + employed.getExtension() + " and " + extension + "." );
					logger.warn("Data consistency problem for [Works - different values found for attribute 'Works.extension': " + employed.getExtension() + " and " + extension + "." );
				}
				if(extension != null)
					employed.setExtension(extension);
				byte[] photo = Util.getByteArrayValue(r.getAs("A_photo"));
				if (employed.getPhoto() != null && photo != null && !employed.getPhoto().equals(photo)) {
					res.addLogEvent("Data consistency problem for [Works - different values found for attribute 'Works.photo': " + employed.getPhoto() + " and " + photo + "." );
					logger.warn("Data consistency problem for [Works - different values found for attribute 'Works.photo': " + employed.getPhoto() + " and " + photo + "." );
				}
				if(photo != null)
					employed.setPhoto(photo);
				String notes = Util.getStringValue(r.getAs("A_notes"));
				if (employed.getNotes() != null && notes != null && !employed.getNotes().equals(notes)) {
					res.addLogEvent("Data consistency problem for [Works - different values found for attribute 'Works.notes': " + employed.getNotes() + " and " + notes + "." );
					logger.warn("Data consistency problem for [Works - different values found for attribute 'Works.notes': " + employed.getNotes() + " and " + notes + "." );
				}
				if(notes != null)
					employed.setNotes(notes);
				String photoPath = Util.getStringValue(r.getAs("A_photoPath"));
				if (employed.getPhotoPath() != null && photoPath != null && !employed.getPhotoPath().equals(photoPath)) {
					res.addLogEvent("Data consistency problem for [Works - different values found for attribute 'Works.photoPath': " + employed.getPhotoPath() + " and " + photoPath + "." );
					logger.warn("Data consistency problem for [Works - different values found for attribute 'Works.photoPath': " + employed.getPhotoPath() + " and " + photoPath + "." );
				}
				if(photoPath != null)
					employed.setPhotoPath(photoPath);
				Double salary = Util.getDoubleValue(r.getAs("A_salary"));
				if (employed.getSalary() != null && salary != null && !employed.getSalary().equals(salary)) {
					res.addLogEvent("Data consistency problem for [Works - different values found for attribute 'Works.salary': " + employed.getSalary() + " and " + salary + "." );
					logger.warn("Data consistency problem for [Works - different values found for attribute 'Works.salary': " + employed.getSalary() + " and " + salary + "." );
				}
				if(salary != null)
					employed.setSalary(salary);
	
				o = r.getAs("territories");
				Territories territories = new Territories();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						territories.setTerritoryID(Util.getStringValue(r2.getAs("territoryID")));
						territories.setTerritoryDescription(Util.getStringValue(r2.getAs("territoryDescription")));
					} 
					if(o instanceof Territories) {
						territories = (Territories) o;
					}
				}
	
				res.setTerritories(territories);
	
				return res;
		}, Encoders.bean(Works.class));
	
		
		
	}
	public static Dataset<Works> fullLeftOuterJoinBetweenWorksAndTerritories(Dataset<Works> d1, Dataset<Territories> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("territoryID", "A_territoryID")
			.withColumnRenamed("territoryDescription", "A_territoryDescription")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("territories.territoryID").equalTo(d2_.col("A_territoryID"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Works>) r -> {
				Works res = new Works();
	
				Territories territories = new Territories();
				Object o = r.getAs("territories");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						territories.setTerritoryID(Util.getStringValue(r2.getAs("territoryID")));
						territories.setTerritoryDescription(Util.getStringValue(r2.getAs("territoryDescription")));
					} 
					if(o instanceof Territories) {
						territories = (Territories) o;
					}
				}
	
				res.setTerritories(territories);
	
				String territoryID = Util.getStringValue(r.getAs("A_territoryID"));
				if (territories.getTerritoryID() != null && territoryID != null && !territories.getTerritoryID().equals(territoryID)) {
					res.addLogEvent("Data consistency problem for [Works - different values found for attribute 'Works.territoryID': " + territories.getTerritoryID() + " and " + territoryID + "." );
					logger.warn("Data consistency problem for [Works - different values found for attribute 'Works.territoryID': " + territories.getTerritoryID() + " and " + territoryID + "." );
				}
				if(territoryID != null)
					territories.setTerritoryID(territoryID);
				String territoryDescription = Util.getStringValue(r.getAs("A_territoryDescription"));
				if (territories.getTerritoryDescription() != null && territoryDescription != null && !territories.getTerritoryDescription().equals(territoryDescription)) {
					res.addLogEvent("Data consistency problem for [Works - different values found for attribute 'Works.territoryDescription': " + territories.getTerritoryDescription() + " and " + territoryDescription + "." );
					logger.warn("Data consistency problem for [Works - different values found for attribute 'Works.territoryDescription': " + territories.getTerritoryDescription() + " and " + territoryDescription + "." );
				}
				if(territoryDescription != null)
					territories.setTerritoryDescription(territoryDescription);
	
				o = r.getAs("employed");
				Employees employed = new Employees();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						employed.setEmployeeID(Util.getIntegerValue(r2.getAs("employeeID")));
						employed.setLastName(Util.getStringValue(r2.getAs("lastName")));
						employed.setFirstName(Util.getStringValue(r2.getAs("firstName")));
						employed.setTitle(Util.getStringValue(r2.getAs("title")));
						employed.setTitleOfCourtesy(Util.getStringValue(r2.getAs("titleOfCourtesy")));
						employed.setBirthDate(Util.getLocalDateValue(r2.getAs("birthDate")));
						employed.setHireDate(Util.getLocalDateValue(r2.getAs("hireDate")));
						employed.setAddress(Util.getStringValue(r2.getAs("address")));
						employed.setCity(Util.getStringValue(r2.getAs("city")));
						employed.setRegion(Util.getStringValue(r2.getAs("region")));
						employed.setPostalCode(Util.getStringValue(r2.getAs("postalCode")));
						employed.setCountry(Util.getStringValue(r2.getAs("country")));
						employed.setHomePhone(Util.getStringValue(r2.getAs("homePhone")));
						employed.setExtension(Util.getStringValue(r2.getAs("extension")));
						employed.setPhoto(Util.getByteArrayValue(r2.getAs("photo")));
						employed.setNotes(Util.getStringValue(r2.getAs("notes")));
						employed.setPhotoPath(Util.getStringValue(r2.getAs("photoPath")));
						employed.setSalary(Util.getDoubleValue(r2.getAs("salary")));
					} 
					if(o instanceof Employees) {
						employed = (Employees) o;
					}
				}
	
				res.setEmployed(employed);
	
				return res;
		}, Encoders.bean(Works.class));
	
		
		
	}
	
	public static Dataset<Works> fullOuterJoinsWorks(List<Dataset<Works>> datasetsPOJO) {
		return fullOuterJoinsWorks(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Works> fullLeftOuterJoinsWorks(List<Dataset<Works>> datasetsPOJO) {
		return fullOuterJoinsWorks(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Works> fullOuterJoinsWorks(List<Dataset<Works>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		List<String> idFields = new ArrayList<String>();
		idFields.add("employed.employeeID");
	
		idFields.add("territories.territoryID");
		scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
		
		List<Dataset<Row>> rows = new ArrayList<Dataset<Row>>();
		for(int i = 0; i < datasetsPOJO.size(); i++) {
			Dataset<Works> d = datasetsPOJO.get(i);
			rows.add(d
				.withColumn("employed_employeeID_" + i, d.col("employed.employeeID"))
				.withColumn("territories_territoryID_" + i, d.col("territories.territoryID"))
				.withColumnRenamed("employed", "employed_" + i)
				.withColumnRenamed("territories", "territories_" + i)
				.withColumnRenamed("logEvents", "logEvents_" + i));
		}
		
		Column joinCond;
		joinCond = rows.get(0).col("employed_employeeID_0").equalTo(rows.get(1).col("employed_employeeID_1"));
		joinCond = joinCond.and(rows.get(0).col("territories_territoryID_0").equalTo(rows.get(1).col("territories_territoryID_1")));
		
		Dataset<Row> res = rows.get(0).join(rows.get(1), joinCond, joinMode);
		for(int i = 2; i < rows.size(); i++) {
			joinCond = rows.get(i - 1).col("employed_employeeID_" + (i - 1)).equalTo(rows.get(i).col("employed_employeeID_" + i));
			joinCond = joinCond.and(rows.get(i - 1).col("territories_territoryID_" + (i - 1)).equalTo(rows.get(i).col("territories_territoryID_" + i)));
			res = res.join(rows.get(i), joinCond, joinMode);
		}
	
		return res.map((MapFunction<Row, Works>) r -> {
				Works works_res = new Works();
	
					WrappedArray logEvents = r.getAs("logEvents_0");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							works_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							works_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					Employees employed_res = new Employees();
					Territories territories_res = new Territories();
					
					// attribute 'Employees.employeeID'
					Integer firstNotNull_employed_employeeID = Util.getIntegerValue(r.getAs("employed_0.employeeID"));
					employed_res.setEmployeeID(firstNotNull_employed_employeeID);
					// attribute 'Employees.lastName'
					String firstNotNull_employed_lastName = Util.getStringValue(r.getAs("employed_0.lastName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employed_lastName2 = Util.getStringValue(r.getAs("employed_" + i + ".lastName"));
						if (firstNotNull_employed_lastName != null && employed_lastName2 != null && !firstNotNull_employed_lastName.equals(employed_lastName2)) {
							works_res.addLogEvent("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.lastName': " + firstNotNull_employed_lastName + " and " + employed_lastName2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.lastName': " + firstNotNull_employed_lastName + " and " + employed_lastName2 + "." );
						}
						if (firstNotNull_employed_lastName == null && employed_lastName2 != null) {
							firstNotNull_employed_lastName = employed_lastName2;
						}
					}
					employed_res.setLastName(firstNotNull_employed_lastName);
					// attribute 'Employees.firstName'
					String firstNotNull_employed_firstName = Util.getStringValue(r.getAs("employed_0.firstName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employed_firstName2 = Util.getStringValue(r.getAs("employed_" + i + ".firstName"));
						if (firstNotNull_employed_firstName != null && employed_firstName2 != null && !firstNotNull_employed_firstName.equals(employed_firstName2)) {
							works_res.addLogEvent("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.firstName': " + firstNotNull_employed_firstName + " and " + employed_firstName2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.firstName': " + firstNotNull_employed_firstName + " and " + employed_firstName2 + "." );
						}
						if (firstNotNull_employed_firstName == null && employed_firstName2 != null) {
							firstNotNull_employed_firstName = employed_firstName2;
						}
					}
					employed_res.setFirstName(firstNotNull_employed_firstName);
					// attribute 'Employees.title'
					String firstNotNull_employed_title = Util.getStringValue(r.getAs("employed_0.title"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employed_title2 = Util.getStringValue(r.getAs("employed_" + i + ".title"));
						if (firstNotNull_employed_title != null && employed_title2 != null && !firstNotNull_employed_title.equals(employed_title2)) {
							works_res.addLogEvent("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.title': " + firstNotNull_employed_title + " and " + employed_title2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.title': " + firstNotNull_employed_title + " and " + employed_title2 + "." );
						}
						if (firstNotNull_employed_title == null && employed_title2 != null) {
							firstNotNull_employed_title = employed_title2;
						}
					}
					employed_res.setTitle(firstNotNull_employed_title);
					// attribute 'Employees.titleOfCourtesy'
					String firstNotNull_employed_titleOfCourtesy = Util.getStringValue(r.getAs("employed_0.titleOfCourtesy"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employed_titleOfCourtesy2 = Util.getStringValue(r.getAs("employed_" + i + ".titleOfCourtesy"));
						if (firstNotNull_employed_titleOfCourtesy != null && employed_titleOfCourtesy2 != null && !firstNotNull_employed_titleOfCourtesy.equals(employed_titleOfCourtesy2)) {
							works_res.addLogEvent("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.titleOfCourtesy': " + firstNotNull_employed_titleOfCourtesy + " and " + employed_titleOfCourtesy2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.titleOfCourtesy': " + firstNotNull_employed_titleOfCourtesy + " and " + employed_titleOfCourtesy2 + "." );
						}
						if (firstNotNull_employed_titleOfCourtesy == null && employed_titleOfCourtesy2 != null) {
							firstNotNull_employed_titleOfCourtesy = employed_titleOfCourtesy2;
						}
					}
					employed_res.setTitleOfCourtesy(firstNotNull_employed_titleOfCourtesy);
					// attribute 'Employees.birthDate'
					LocalDate firstNotNull_employed_birthDate = Util.getLocalDateValue(r.getAs("employed_0.birthDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate employed_birthDate2 = Util.getLocalDateValue(r.getAs("employed_" + i + ".birthDate"));
						if (firstNotNull_employed_birthDate != null && employed_birthDate2 != null && !firstNotNull_employed_birthDate.equals(employed_birthDate2)) {
							works_res.addLogEvent("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.birthDate': " + firstNotNull_employed_birthDate + " and " + employed_birthDate2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.birthDate': " + firstNotNull_employed_birthDate + " and " + employed_birthDate2 + "." );
						}
						if (firstNotNull_employed_birthDate == null && employed_birthDate2 != null) {
							firstNotNull_employed_birthDate = employed_birthDate2;
						}
					}
					employed_res.setBirthDate(firstNotNull_employed_birthDate);
					// attribute 'Employees.hireDate'
					LocalDate firstNotNull_employed_hireDate = Util.getLocalDateValue(r.getAs("employed_0.hireDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate employed_hireDate2 = Util.getLocalDateValue(r.getAs("employed_" + i + ".hireDate"));
						if (firstNotNull_employed_hireDate != null && employed_hireDate2 != null && !firstNotNull_employed_hireDate.equals(employed_hireDate2)) {
							works_res.addLogEvent("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.hireDate': " + firstNotNull_employed_hireDate + " and " + employed_hireDate2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.hireDate': " + firstNotNull_employed_hireDate + " and " + employed_hireDate2 + "." );
						}
						if (firstNotNull_employed_hireDate == null && employed_hireDate2 != null) {
							firstNotNull_employed_hireDate = employed_hireDate2;
						}
					}
					employed_res.setHireDate(firstNotNull_employed_hireDate);
					// attribute 'Employees.address'
					String firstNotNull_employed_address = Util.getStringValue(r.getAs("employed_0.address"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employed_address2 = Util.getStringValue(r.getAs("employed_" + i + ".address"));
						if (firstNotNull_employed_address != null && employed_address2 != null && !firstNotNull_employed_address.equals(employed_address2)) {
							works_res.addLogEvent("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.address': " + firstNotNull_employed_address + " and " + employed_address2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.address': " + firstNotNull_employed_address + " and " + employed_address2 + "." );
						}
						if (firstNotNull_employed_address == null && employed_address2 != null) {
							firstNotNull_employed_address = employed_address2;
						}
					}
					employed_res.setAddress(firstNotNull_employed_address);
					// attribute 'Employees.city'
					String firstNotNull_employed_city = Util.getStringValue(r.getAs("employed_0.city"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employed_city2 = Util.getStringValue(r.getAs("employed_" + i + ".city"));
						if (firstNotNull_employed_city != null && employed_city2 != null && !firstNotNull_employed_city.equals(employed_city2)) {
							works_res.addLogEvent("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.city': " + firstNotNull_employed_city + " and " + employed_city2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.city': " + firstNotNull_employed_city + " and " + employed_city2 + "." );
						}
						if (firstNotNull_employed_city == null && employed_city2 != null) {
							firstNotNull_employed_city = employed_city2;
						}
					}
					employed_res.setCity(firstNotNull_employed_city);
					// attribute 'Employees.region'
					String firstNotNull_employed_region = Util.getStringValue(r.getAs("employed_0.region"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employed_region2 = Util.getStringValue(r.getAs("employed_" + i + ".region"));
						if (firstNotNull_employed_region != null && employed_region2 != null && !firstNotNull_employed_region.equals(employed_region2)) {
							works_res.addLogEvent("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.region': " + firstNotNull_employed_region + " and " + employed_region2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.region': " + firstNotNull_employed_region + " and " + employed_region2 + "." );
						}
						if (firstNotNull_employed_region == null && employed_region2 != null) {
							firstNotNull_employed_region = employed_region2;
						}
					}
					employed_res.setRegion(firstNotNull_employed_region);
					// attribute 'Employees.postalCode'
					String firstNotNull_employed_postalCode = Util.getStringValue(r.getAs("employed_0.postalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employed_postalCode2 = Util.getStringValue(r.getAs("employed_" + i + ".postalCode"));
						if (firstNotNull_employed_postalCode != null && employed_postalCode2 != null && !firstNotNull_employed_postalCode.equals(employed_postalCode2)) {
							works_res.addLogEvent("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.postalCode': " + firstNotNull_employed_postalCode + " and " + employed_postalCode2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.postalCode': " + firstNotNull_employed_postalCode + " and " + employed_postalCode2 + "." );
						}
						if (firstNotNull_employed_postalCode == null && employed_postalCode2 != null) {
							firstNotNull_employed_postalCode = employed_postalCode2;
						}
					}
					employed_res.setPostalCode(firstNotNull_employed_postalCode);
					// attribute 'Employees.country'
					String firstNotNull_employed_country = Util.getStringValue(r.getAs("employed_0.country"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employed_country2 = Util.getStringValue(r.getAs("employed_" + i + ".country"));
						if (firstNotNull_employed_country != null && employed_country2 != null && !firstNotNull_employed_country.equals(employed_country2)) {
							works_res.addLogEvent("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.country': " + firstNotNull_employed_country + " and " + employed_country2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.country': " + firstNotNull_employed_country + " and " + employed_country2 + "." );
						}
						if (firstNotNull_employed_country == null && employed_country2 != null) {
							firstNotNull_employed_country = employed_country2;
						}
					}
					employed_res.setCountry(firstNotNull_employed_country);
					// attribute 'Employees.homePhone'
					String firstNotNull_employed_homePhone = Util.getStringValue(r.getAs("employed_0.homePhone"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employed_homePhone2 = Util.getStringValue(r.getAs("employed_" + i + ".homePhone"));
						if (firstNotNull_employed_homePhone != null && employed_homePhone2 != null && !firstNotNull_employed_homePhone.equals(employed_homePhone2)) {
							works_res.addLogEvent("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.homePhone': " + firstNotNull_employed_homePhone + " and " + employed_homePhone2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.homePhone': " + firstNotNull_employed_homePhone + " and " + employed_homePhone2 + "." );
						}
						if (firstNotNull_employed_homePhone == null && employed_homePhone2 != null) {
							firstNotNull_employed_homePhone = employed_homePhone2;
						}
					}
					employed_res.setHomePhone(firstNotNull_employed_homePhone);
					// attribute 'Employees.extension'
					String firstNotNull_employed_extension = Util.getStringValue(r.getAs("employed_0.extension"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employed_extension2 = Util.getStringValue(r.getAs("employed_" + i + ".extension"));
						if (firstNotNull_employed_extension != null && employed_extension2 != null && !firstNotNull_employed_extension.equals(employed_extension2)) {
							works_res.addLogEvent("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.extension': " + firstNotNull_employed_extension + " and " + employed_extension2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.extension': " + firstNotNull_employed_extension + " and " + employed_extension2 + "." );
						}
						if (firstNotNull_employed_extension == null && employed_extension2 != null) {
							firstNotNull_employed_extension = employed_extension2;
						}
					}
					employed_res.setExtension(firstNotNull_employed_extension);
					// attribute 'Employees.photo'
					byte[] firstNotNull_employed_photo = Util.getByteArrayValue(r.getAs("employed_0.photo"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						byte[] employed_photo2 = Util.getByteArrayValue(r.getAs("employed_" + i + ".photo"));
						if (firstNotNull_employed_photo != null && employed_photo2 != null && !firstNotNull_employed_photo.equals(employed_photo2)) {
							works_res.addLogEvent("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.photo': " + firstNotNull_employed_photo + " and " + employed_photo2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.photo': " + firstNotNull_employed_photo + " and " + employed_photo2 + "." );
						}
						if (firstNotNull_employed_photo == null && employed_photo2 != null) {
							firstNotNull_employed_photo = employed_photo2;
						}
					}
					employed_res.setPhoto(firstNotNull_employed_photo);
					// attribute 'Employees.notes'
					String firstNotNull_employed_notes = Util.getStringValue(r.getAs("employed_0.notes"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employed_notes2 = Util.getStringValue(r.getAs("employed_" + i + ".notes"));
						if (firstNotNull_employed_notes != null && employed_notes2 != null && !firstNotNull_employed_notes.equals(employed_notes2)) {
							works_res.addLogEvent("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.notes': " + firstNotNull_employed_notes + " and " + employed_notes2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.notes': " + firstNotNull_employed_notes + " and " + employed_notes2 + "." );
						}
						if (firstNotNull_employed_notes == null && employed_notes2 != null) {
							firstNotNull_employed_notes = employed_notes2;
						}
					}
					employed_res.setNotes(firstNotNull_employed_notes);
					// attribute 'Employees.photoPath'
					String firstNotNull_employed_photoPath = Util.getStringValue(r.getAs("employed_0.photoPath"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employed_photoPath2 = Util.getStringValue(r.getAs("employed_" + i + ".photoPath"));
						if (firstNotNull_employed_photoPath != null && employed_photoPath2 != null && !firstNotNull_employed_photoPath.equals(employed_photoPath2)) {
							works_res.addLogEvent("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.photoPath': " + firstNotNull_employed_photoPath + " and " + employed_photoPath2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.photoPath': " + firstNotNull_employed_photoPath + " and " + employed_photoPath2 + "." );
						}
						if (firstNotNull_employed_photoPath == null && employed_photoPath2 != null) {
							firstNotNull_employed_photoPath = employed_photoPath2;
						}
					}
					employed_res.setPhotoPath(firstNotNull_employed_photoPath);
					// attribute 'Employees.salary'
					Double firstNotNull_employed_salary = Util.getDoubleValue(r.getAs("employed_0.salary"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double employed_salary2 = Util.getDoubleValue(r.getAs("employed_" + i + ".salary"));
						if (firstNotNull_employed_salary != null && employed_salary2 != null && !firstNotNull_employed_salary.equals(employed_salary2)) {
							works_res.addLogEvent("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.salary': " + firstNotNull_employed_salary + " and " + employed_salary2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employed_res.getEmployeeID()+"]: different values found for attribute 'Employees.salary': " + firstNotNull_employed_salary + " and " + employed_salary2 + "." );
						}
						if (firstNotNull_employed_salary == null && employed_salary2 != null) {
							firstNotNull_employed_salary = employed_salary2;
						}
					}
					employed_res.setSalary(firstNotNull_employed_salary);
					// attribute 'Territories.territoryID'
					String firstNotNull_territories_territoryID = Util.getStringValue(r.getAs("territories_0.territoryID"));
					territories_res.setTerritoryID(firstNotNull_territories_territoryID);
					// attribute 'Territories.territoryDescription'
					String firstNotNull_territories_territoryDescription = Util.getStringValue(r.getAs("territories_0.territoryDescription"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String territories_territoryDescription2 = Util.getStringValue(r.getAs("territories_" + i + ".territoryDescription"));
						if (firstNotNull_territories_territoryDescription != null && territories_territoryDescription2 != null && !firstNotNull_territories_territoryDescription.equals(territories_territoryDescription2)) {
							works_res.addLogEvent("Data consistency problem for [Territories - id :"+territories_res.getTerritoryID()+"]: different values found for attribute 'Territories.territoryDescription': " + firstNotNull_territories_territoryDescription + " and " + territories_territoryDescription2 + "." );
							logger.warn("Data consistency problem for [Territories - id :"+territories_res.getTerritoryID()+"]: different values found for attribute 'Territories.territoryDescription': " + firstNotNull_territories_territoryDescription + " and " + territories_territoryDescription2 + "." );
						}
						if (firstNotNull_territories_territoryDescription == null && territories_territoryDescription2 != null) {
							firstNotNull_territories_territoryDescription = territories_territoryDescription2;
						}
					}
					territories_res.setTerritoryDescription(firstNotNull_territories_territoryDescription);
	
					works_res.setEmployed(employed_res);
					works_res.setTerritories(territories_res);
					return works_res;
		}
		, Encoders.bean(Works.class));
	
	}
	
	//Empty arguments
	public Dataset<Works> getWorksList(){
		 return getWorksList(null,null);
	}
	
	public abstract Dataset<Works> getWorksList(
		Condition<EmployeesAttribute> employed_condition,
		Condition<TerritoriesAttribute> territories_condition);
	
	public Dataset<Works> getWorksListByEmployedCondition(
		Condition<EmployeesAttribute> employed_condition
	){
		return getWorksList(employed_condition, null);
	}
	
	public Dataset<Works> getWorksListByEmployed(Employees employed) {
		Condition<EmployeesAttribute> cond = null;
		cond = Condition.simple(EmployeesAttribute.employeeID, Operator.EQUALS, employed.getEmployeeID());
		Dataset<Works> res = getWorksListByEmployedCondition(cond);
	return res;
	}
	public Dataset<Works> getWorksListByTerritoriesCondition(
		Condition<TerritoriesAttribute> territories_condition
	){
		return getWorksList(null, territories_condition);
	}
	
	public Dataset<Works> getWorksListByTerritories(Territories territories) {
		Condition<TerritoriesAttribute> cond = null;
		cond = Condition.simple(TerritoriesAttribute.territoryID, Operator.EQUALS, territories.getTerritoryID());
		Dataset<Works> res = getWorksListByTerritoriesCondition(cond);
	return res;
	}
	
	public abstract void insertWorks(Works works);
	
	public 	abstract boolean insertWorksInJoinStructEmployeeTerritoriesInMyRelDB(Works works);
	
	
	
	 public void insertWorks(Employees employed ,Territories territories ){
		Works works = new Works();
		works.setEmployed(employed);
		works.setTerritories(territories);
		insertWorks(works);
	}
	
	 public void insertWorks(Territories territories, List<Employees> employedList){
		Works works = new Works();
		works.setTerritories(territories);
		for(Employees employed : employedList){
			works.setEmployed(employed);
			insertWorks(works);
		}
	}
	 public void insertWorks(Employees employees, List<Territories> territoriesList){
		Works works = new Works();
		works.setEmployed(employees);
		for(Territories territories : territoriesList){
			works.setTerritories(territories);
			insertWorks(works);
		}
	}
	
	
	public abstract void deleteWorksList(
		conditions.Condition<conditions.EmployeesAttribute> employed_condition,
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition);
	
	public void deleteWorksListByEmployedCondition(
		conditions.Condition<conditions.EmployeesAttribute> employed_condition
	){
		deleteWorksList(employed_condition, null);
	}
	
	public void deleteWorksListByEmployed(pojo.Employees employed) {
		// TODO using id for selecting
		return;
	}
	public void deleteWorksListByTerritoriesCondition(
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition
	){
		deleteWorksList(null, territories_condition);
	}
	
	public void deleteWorksListByTerritories(pojo.Territories territories) {
		// TODO using id for selecting
		return;
	}
		
}
