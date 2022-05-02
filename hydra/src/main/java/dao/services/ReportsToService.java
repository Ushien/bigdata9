package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import conditions.*;
import pojo.ReportsTo;
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


public abstract class ReportsToService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReportsToService.class);
	
	public abstract Dataset<ReportsTo> getSubordoneeAndBossListInManagerInEmployeesFromMyRelDB(Condition<EmployeesAttribute> subordonee_cond, Condition<EmployeesAttribute> boss_cond, MutableBoolean subordonee_refilter, MutableBoolean boss_refilter);
	
	
	
	
	
	public static Dataset<ReportsTo> fullLeftOuterJoinBetweenReportsToAndSubordonee(Dataset<ReportsTo> d1, Dataset<Employees> d2) {
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
		joinCond = d1.col("subordonee.employeeID").equalTo(d2_.col("A_employeeID"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, ReportsTo>) r -> {
				ReportsTo res = new ReportsTo();
	
				Employees subordonee = new Employees();
				Object o = r.getAs("subordonee");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						subordonee.setEmployeeID(Util.getIntegerValue(r2.getAs("employeeID")));
						subordonee.setLastName(Util.getStringValue(r2.getAs("lastName")));
						subordonee.setFirstName(Util.getStringValue(r2.getAs("firstName")));
						subordonee.setTitle(Util.getStringValue(r2.getAs("title")));
						subordonee.setTitleOfCourtesy(Util.getStringValue(r2.getAs("titleOfCourtesy")));
						subordonee.setBirthDate(Util.getLocalDateValue(r2.getAs("birthDate")));
						subordonee.setHireDate(Util.getLocalDateValue(r2.getAs("hireDate")));
						subordonee.setAddress(Util.getStringValue(r2.getAs("address")));
						subordonee.setCity(Util.getStringValue(r2.getAs("city")));
						subordonee.setRegion(Util.getStringValue(r2.getAs("region")));
						subordonee.setPostalCode(Util.getStringValue(r2.getAs("postalCode")));
						subordonee.setCountry(Util.getStringValue(r2.getAs("country")));
						subordonee.setHomePhone(Util.getStringValue(r2.getAs("homePhone")));
						subordonee.setExtension(Util.getStringValue(r2.getAs("extension")));
						subordonee.setPhoto(Util.getByteArrayValue(r2.getAs("photo")));
						subordonee.setNotes(Util.getStringValue(r2.getAs("notes")));
						subordonee.setPhotoPath(Util.getStringValue(r2.getAs("photoPath")));
						subordonee.setSalary(Util.getDoubleValue(r2.getAs("salary")));
					} 
					if(o instanceof Employees) {
						subordonee = (Employees) o;
					}
				}
	
				res.setSubordonee(subordonee);
	
				Integer employeeID = Util.getIntegerValue(r.getAs("A_employeeID"));
				if (subordonee.getEmployeeID() != null && employeeID != null && !subordonee.getEmployeeID().equals(employeeID)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.employeeID': " + subordonee.getEmployeeID() + " and " + employeeID + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.employeeID': " + subordonee.getEmployeeID() + " and " + employeeID + "." );
				}
				if(employeeID != null)
					subordonee.setEmployeeID(employeeID);
				String lastName = Util.getStringValue(r.getAs("A_lastName"));
				if (subordonee.getLastName() != null && lastName != null && !subordonee.getLastName().equals(lastName)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.lastName': " + subordonee.getLastName() + " and " + lastName + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.lastName': " + subordonee.getLastName() + " and " + lastName + "." );
				}
				if(lastName != null)
					subordonee.setLastName(lastName);
				String firstName = Util.getStringValue(r.getAs("A_firstName"));
				if (subordonee.getFirstName() != null && firstName != null && !subordonee.getFirstName().equals(firstName)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.firstName': " + subordonee.getFirstName() + " and " + firstName + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.firstName': " + subordonee.getFirstName() + " and " + firstName + "." );
				}
				if(firstName != null)
					subordonee.setFirstName(firstName);
				String title = Util.getStringValue(r.getAs("A_title"));
				if (subordonee.getTitle() != null && title != null && !subordonee.getTitle().equals(title)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.title': " + subordonee.getTitle() + " and " + title + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.title': " + subordonee.getTitle() + " and " + title + "." );
				}
				if(title != null)
					subordonee.setTitle(title);
				String titleOfCourtesy = Util.getStringValue(r.getAs("A_titleOfCourtesy"));
				if (subordonee.getTitleOfCourtesy() != null && titleOfCourtesy != null && !subordonee.getTitleOfCourtesy().equals(titleOfCourtesy)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.titleOfCourtesy': " + subordonee.getTitleOfCourtesy() + " and " + titleOfCourtesy + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.titleOfCourtesy': " + subordonee.getTitleOfCourtesy() + " and " + titleOfCourtesy + "." );
				}
				if(titleOfCourtesy != null)
					subordonee.setTitleOfCourtesy(titleOfCourtesy);
				LocalDate birthDate = Util.getLocalDateValue(r.getAs("A_birthDate"));
				if (subordonee.getBirthDate() != null && birthDate != null && !subordonee.getBirthDate().equals(birthDate)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.birthDate': " + subordonee.getBirthDate() + " and " + birthDate + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.birthDate': " + subordonee.getBirthDate() + " and " + birthDate + "." );
				}
				if(birthDate != null)
					subordonee.setBirthDate(birthDate);
				LocalDate hireDate = Util.getLocalDateValue(r.getAs("A_hireDate"));
				if (subordonee.getHireDate() != null && hireDate != null && !subordonee.getHireDate().equals(hireDate)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.hireDate': " + subordonee.getHireDate() + " and " + hireDate + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.hireDate': " + subordonee.getHireDate() + " and " + hireDate + "." );
				}
				if(hireDate != null)
					subordonee.setHireDate(hireDate);
				String address = Util.getStringValue(r.getAs("A_address"));
				if (subordonee.getAddress() != null && address != null && !subordonee.getAddress().equals(address)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.address': " + subordonee.getAddress() + " and " + address + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.address': " + subordonee.getAddress() + " and " + address + "." );
				}
				if(address != null)
					subordonee.setAddress(address);
				String city = Util.getStringValue(r.getAs("A_city"));
				if (subordonee.getCity() != null && city != null && !subordonee.getCity().equals(city)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.city': " + subordonee.getCity() + " and " + city + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.city': " + subordonee.getCity() + " and " + city + "." );
				}
				if(city != null)
					subordonee.setCity(city);
				String region = Util.getStringValue(r.getAs("A_region"));
				if (subordonee.getRegion() != null && region != null && !subordonee.getRegion().equals(region)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.region': " + subordonee.getRegion() + " and " + region + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.region': " + subordonee.getRegion() + " and " + region + "." );
				}
				if(region != null)
					subordonee.setRegion(region);
				String postalCode = Util.getStringValue(r.getAs("A_postalCode"));
				if (subordonee.getPostalCode() != null && postalCode != null && !subordonee.getPostalCode().equals(postalCode)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.postalCode': " + subordonee.getPostalCode() + " and " + postalCode + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.postalCode': " + subordonee.getPostalCode() + " and " + postalCode + "." );
				}
				if(postalCode != null)
					subordonee.setPostalCode(postalCode);
				String country = Util.getStringValue(r.getAs("A_country"));
				if (subordonee.getCountry() != null && country != null && !subordonee.getCountry().equals(country)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.country': " + subordonee.getCountry() + " and " + country + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.country': " + subordonee.getCountry() + " and " + country + "." );
				}
				if(country != null)
					subordonee.setCountry(country);
				String homePhone = Util.getStringValue(r.getAs("A_homePhone"));
				if (subordonee.getHomePhone() != null && homePhone != null && !subordonee.getHomePhone().equals(homePhone)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.homePhone': " + subordonee.getHomePhone() + " and " + homePhone + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.homePhone': " + subordonee.getHomePhone() + " and " + homePhone + "." );
				}
				if(homePhone != null)
					subordonee.setHomePhone(homePhone);
				String extension = Util.getStringValue(r.getAs("A_extension"));
				if (subordonee.getExtension() != null && extension != null && !subordonee.getExtension().equals(extension)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.extension': " + subordonee.getExtension() + " and " + extension + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.extension': " + subordonee.getExtension() + " and " + extension + "." );
				}
				if(extension != null)
					subordonee.setExtension(extension);
				byte[] photo = Util.getByteArrayValue(r.getAs("A_photo"));
				if (subordonee.getPhoto() != null && photo != null && !subordonee.getPhoto().equals(photo)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.photo': " + subordonee.getPhoto() + " and " + photo + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.photo': " + subordonee.getPhoto() + " and " + photo + "." );
				}
				if(photo != null)
					subordonee.setPhoto(photo);
				String notes = Util.getStringValue(r.getAs("A_notes"));
				if (subordonee.getNotes() != null && notes != null && !subordonee.getNotes().equals(notes)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.notes': " + subordonee.getNotes() + " and " + notes + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.notes': " + subordonee.getNotes() + " and " + notes + "." );
				}
				if(notes != null)
					subordonee.setNotes(notes);
				String photoPath = Util.getStringValue(r.getAs("A_photoPath"));
				if (subordonee.getPhotoPath() != null && photoPath != null && !subordonee.getPhotoPath().equals(photoPath)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.photoPath': " + subordonee.getPhotoPath() + " and " + photoPath + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.photoPath': " + subordonee.getPhotoPath() + " and " + photoPath + "." );
				}
				if(photoPath != null)
					subordonee.setPhotoPath(photoPath);
				Double salary = Util.getDoubleValue(r.getAs("A_salary"));
				if (subordonee.getSalary() != null && salary != null && !subordonee.getSalary().equals(salary)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.salary': " + subordonee.getSalary() + " and " + salary + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.salary': " + subordonee.getSalary() + " and " + salary + "." );
				}
				if(salary != null)
					subordonee.setSalary(salary);
	
				o = r.getAs("boss");
				Employees boss = new Employees();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						boss.setEmployeeID(Util.getIntegerValue(r2.getAs("employeeID")));
						boss.setLastName(Util.getStringValue(r2.getAs("lastName")));
						boss.setFirstName(Util.getStringValue(r2.getAs("firstName")));
						boss.setTitle(Util.getStringValue(r2.getAs("title")));
						boss.setTitleOfCourtesy(Util.getStringValue(r2.getAs("titleOfCourtesy")));
						boss.setBirthDate(Util.getLocalDateValue(r2.getAs("birthDate")));
						boss.setHireDate(Util.getLocalDateValue(r2.getAs("hireDate")));
						boss.setAddress(Util.getStringValue(r2.getAs("address")));
						boss.setCity(Util.getStringValue(r2.getAs("city")));
						boss.setRegion(Util.getStringValue(r2.getAs("region")));
						boss.setPostalCode(Util.getStringValue(r2.getAs("postalCode")));
						boss.setCountry(Util.getStringValue(r2.getAs("country")));
						boss.setHomePhone(Util.getStringValue(r2.getAs("homePhone")));
						boss.setExtension(Util.getStringValue(r2.getAs("extension")));
						boss.setPhoto(Util.getByteArrayValue(r2.getAs("photo")));
						boss.setNotes(Util.getStringValue(r2.getAs("notes")));
						boss.setPhotoPath(Util.getStringValue(r2.getAs("photoPath")));
						boss.setSalary(Util.getDoubleValue(r2.getAs("salary")));
					} 
					if(o instanceof Employees) {
						boss = (Employees) o;
					}
				}
	
				res.setBoss(boss);
	
				return res;
		}, Encoders.bean(ReportsTo.class));
	
		
		
	}
	public static Dataset<ReportsTo> fullLeftOuterJoinBetweenReportsToAndBoss(Dataset<ReportsTo> d1, Dataset<Employees> d2) {
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
		joinCond = d1.col("boss.employeeID").equalTo(d2_.col("A_employeeID"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, ReportsTo>) r -> {
				ReportsTo res = new ReportsTo();
	
				Employees boss = new Employees();
				Object o = r.getAs("boss");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						boss.setEmployeeID(Util.getIntegerValue(r2.getAs("employeeID")));
						boss.setLastName(Util.getStringValue(r2.getAs("lastName")));
						boss.setFirstName(Util.getStringValue(r2.getAs("firstName")));
						boss.setTitle(Util.getStringValue(r2.getAs("title")));
						boss.setTitleOfCourtesy(Util.getStringValue(r2.getAs("titleOfCourtesy")));
						boss.setBirthDate(Util.getLocalDateValue(r2.getAs("birthDate")));
						boss.setHireDate(Util.getLocalDateValue(r2.getAs("hireDate")));
						boss.setAddress(Util.getStringValue(r2.getAs("address")));
						boss.setCity(Util.getStringValue(r2.getAs("city")));
						boss.setRegion(Util.getStringValue(r2.getAs("region")));
						boss.setPostalCode(Util.getStringValue(r2.getAs("postalCode")));
						boss.setCountry(Util.getStringValue(r2.getAs("country")));
						boss.setHomePhone(Util.getStringValue(r2.getAs("homePhone")));
						boss.setExtension(Util.getStringValue(r2.getAs("extension")));
						boss.setPhoto(Util.getByteArrayValue(r2.getAs("photo")));
						boss.setNotes(Util.getStringValue(r2.getAs("notes")));
						boss.setPhotoPath(Util.getStringValue(r2.getAs("photoPath")));
						boss.setSalary(Util.getDoubleValue(r2.getAs("salary")));
					} 
					if(o instanceof Employees) {
						boss = (Employees) o;
					}
				}
	
				res.setBoss(boss);
	
				Integer employeeID = Util.getIntegerValue(r.getAs("A_employeeID"));
				if (boss.getEmployeeID() != null && employeeID != null && !boss.getEmployeeID().equals(employeeID)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.employeeID': " + boss.getEmployeeID() + " and " + employeeID + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.employeeID': " + boss.getEmployeeID() + " and " + employeeID + "." );
				}
				if(employeeID != null)
					boss.setEmployeeID(employeeID);
				String lastName = Util.getStringValue(r.getAs("A_lastName"));
				if (boss.getLastName() != null && lastName != null && !boss.getLastName().equals(lastName)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.lastName': " + boss.getLastName() + " and " + lastName + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.lastName': " + boss.getLastName() + " and " + lastName + "." );
				}
				if(lastName != null)
					boss.setLastName(lastName);
				String firstName = Util.getStringValue(r.getAs("A_firstName"));
				if (boss.getFirstName() != null && firstName != null && !boss.getFirstName().equals(firstName)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.firstName': " + boss.getFirstName() + " and " + firstName + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.firstName': " + boss.getFirstName() + " and " + firstName + "." );
				}
				if(firstName != null)
					boss.setFirstName(firstName);
				String title = Util.getStringValue(r.getAs("A_title"));
				if (boss.getTitle() != null && title != null && !boss.getTitle().equals(title)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.title': " + boss.getTitle() + " and " + title + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.title': " + boss.getTitle() + " and " + title + "." );
				}
				if(title != null)
					boss.setTitle(title);
				String titleOfCourtesy = Util.getStringValue(r.getAs("A_titleOfCourtesy"));
				if (boss.getTitleOfCourtesy() != null && titleOfCourtesy != null && !boss.getTitleOfCourtesy().equals(titleOfCourtesy)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.titleOfCourtesy': " + boss.getTitleOfCourtesy() + " and " + titleOfCourtesy + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.titleOfCourtesy': " + boss.getTitleOfCourtesy() + " and " + titleOfCourtesy + "." );
				}
				if(titleOfCourtesy != null)
					boss.setTitleOfCourtesy(titleOfCourtesy);
				LocalDate birthDate = Util.getLocalDateValue(r.getAs("A_birthDate"));
				if (boss.getBirthDate() != null && birthDate != null && !boss.getBirthDate().equals(birthDate)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.birthDate': " + boss.getBirthDate() + " and " + birthDate + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.birthDate': " + boss.getBirthDate() + " and " + birthDate + "." );
				}
				if(birthDate != null)
					boss.setBirthDate(birthDate);
				LocalDate hireDate = Util.getLocalDateValue(r.getAs("A_hireDate"));
				if (boss.getHireDate() != null && hireDate != null && !boss.getHireDate().equals(hireDate)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.hireDate': " + boss.getHireDate() + " and " + hireDate + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.hireDate': " + boss.getHireDate() + " and " + hireDate + "." );
				}
				if(hireDate != null)
					boss.setHireDate(hireDate);
				String address = Util.getStringValue(r.getAs("A_address"));
				if (boss.getAddress() != null && address != null && !boss.getAddress().equals(address)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.address': " + boss.getAddress() + " and " + address + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.address': " + boss.getAddress() + " and " + address + "." );
				}
				if(address != null)
					boss.setAddress(address);
				String city = Util.getStringValue(r.getAs("A_city"));
				if (boss.getCity() != null && city != null && !boss.getCity().equals(city)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.city': " + boss.getCity() + " and " + city + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.city': " + boss.getCity() + " and " + city + "." );
				}
				if(city != null)
					boss.setCity(city);
				String region = Util.getStringValue(r.getAs("A_region"));
				if (boss.getRegion() != null && region != null && !boss.getRegion().equals(region)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.region': " + boss.getRegion() + " and " + region + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.region': " + boss.getRegion() + " and " + region + "." );
				}
				if(region != null)
					boss.setRegion(region);
				String postalCode = Util.getStringValue(r.getAs("A_postalCode"));
				if (boss.getPostalCode() != null && postalCode != null && !boss.getPostalCode().equals(postalCode)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.postalCode': " + boss.getPostalCode() + " and " + postalCode + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.postalCode': " + boss.getPostalCode() + " and " + postalCode + "." );
				}
				if(postalCode != null)
					boss.setPostalCode(postalCode);
				String country = Util.getStringValue(r.getAs("A_country"));
				if (boss.getCountry() != null && country != null && !boss.getCountry().equals(country)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.country': " + boss.getCountry() + " and " + country + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.country': " + boss.getCountry() + " and " + country + "." );
				}
				if(country != null)
					boss.setCountry(country);
				String homePhone = Util.getStringValue(r.getAs("A_homePhone"));
				if (boss.getHomePhone() != null && homePhone != null && !boss.getHomePhone().equals(homePhone)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.homePhone': " + boss.getHomePhone() + " and " + homePhone + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.homePhone': " + boss.getHomePhone() + " and " + homePhone + "." );
				}
				if(homePhone != null)
					boss.setHomePhone(homePhone);
				String extension = Util.getStringValue(r.getAs("A_extension"));
				if (boss.getExtension() != null && extension != null && !boss.getExtension().equals(extension)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.extension': " + boss.getExtension() + " and " + extension + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.extension': " + boss.getExtension() + " and " + extension + "." );
				}
				if(extension != null)
					boss.setExtension(extension);
				byte[] photo = Util.getByteArrayValue(r.getAs("A_photo"));
				if (boss.getPhoto() != null && photo != null && !boss.getPhoto().equals(photo)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.photo': " + boss.getPhoto() + " and " + photo + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.photo': " + boss.getPhoto() + " and " + photo + "." );
				}
				if(photo != null)
					boss.setPhoto(photo);
				String notes = Util.getStringValue(r.getAs("A_notes"));
				if (boss.getNotes() != null && notes != null && !boss.getNotes().equals(notes)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.notes': " + boss.getNotes() + " and " + notes + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.notes': " + boss.getNotes() + " and " + notes + "." );
				}
				if(notes != null)
					boss.setNotes(notes);
				String photoPath = Util.getStringValue(r.getAs("A_photoPath"));
				if (boss.getPhotoPath() != null && photoPath != null && !boss.getPhotoPath().equals(photoPath)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.photoPath': " + boss.getPhotoPath() + " and " + photoPath + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.photoPath': " + boss.getPhotoPath() + " and " + photoPath + "." );
				}
				if(photoPath != null)
					boss.setPhotoPath(photoPath);
				Double salary = Util.getDoubleValue(r.getAs("A_salary"));
				if (boss.getSalary() != null && salary != null && !boss.getSalary().equals(salary)) {
					res.addLogEvent("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.salary': " + boss.getSalary() + " and " + salary + "." );
					logger.warn("Data consistency problem for [ReportsTo - different values found for attribute 'ReportsTo.salary': " + boss.getSalary() + " and " + salary + "." );
				}
				if(salary != null)
					boss.setSalary(salary);
	
				o = r.getAs("subordonee");
				Employees subordonee = new Employees();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						subordonee.setEmployeeID(Util.getIntegerValue(r2.getAs("employeeID")));
						subordonee.setLastName(Util.getStringValue(r2.getAs("lastName")));
						subordonee.setFirstName(Util.getStringValue(r2.getAs("firstName")));
						subordonee.setTitle(Util.getStringValue(r2.getAs("title")));
						subordonee.setTitleOfCourtesy(Util.getStringValue(r2.getAs("titleOfCourtesy")));
						subordonee.setBirthDate(Util.getLocalDateValue(r2.getAs("birthDate")));
						subordonee.setHireDate(Util.getLocalDateValue(r2.getAs("hireDate")));
						subordonee.setAddress(Util.getStringValue(r2.getAs("address")));
						subordonee.setCity(Util.getStringValue(r2.getAs("city")));
						subordonee.setRegion(Util.getStringValue(r2.getAs("region")));
						subordonee.setPostalCode(Util.getStringValue(r2.getAs("postalCode")));
						subordonee.setCountry(Util.getStringValue(r2.getAs("country")));
						subordonee.setHomePhone(Util.getStringValue(r2.getAs("homePhone")));
						subordonee.setExtension(Util.getStringValue(r2.getAs("extension")));
						subordonee.setPhoto(Util.getByteArrayValue(r2.getAs("photo")));
						subordonee.setNotes(Util.getStringValue(r2.getAs("notes")));
						subordonee.setPhotoPath(Util.getStringValue(r2.getAs("photoPath")));
						subordonee.setSalary(Util.getDoubleValue(r2.getAs("salary")));
					} 
					if(o instanceof Employees) {
						subordonee = (Employees) o;
					}
				}
	
				res.setSubordonee(subordonee);
	
				return res;
		}, Encoders.bean(ReportsTo.class));
	
		
		
	}
	
	public static Dataset<ReportsTo> fullOuterJoinsReportsTo(List<Dataset<ReportsTo>> datasetsPOJO) {
		return fullOuterJoinsReportsTo(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<ReportsTo> fullLeftOuterJoinsReportsTo(List<Dataset<ReportsTo>> datasetsPOJO) {
		return fullOuterJoinsReportsTo(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<ReportsTo> fullOuterJoinsReportsTo(List<Dataset<ReportsTo>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		List<String> idFields = new ArrayList<String>();
		idFields.add("subordonee.employeeID");
	
		idFields.add("boss.employeeID");
		scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
		
		List<Dataset<Row>> rows = new ArrayList<Dataset<Row>>();
		for(int i = 0; i < datasetsPOJO.size(); i++) {
			Dataset<ReportsTo> d = datasetsPOJO.get(i);
			rows.add(d
				.withColumn("subordonee_employeeID_" + i, d.col("subordonee.employeeID"))
				.withColumn("boss_employeeID_" + i, d.col("boss.employeeID"))
				.withColumnRenamed("subordonee", "subordonee_" + i)
				.withColumnRenamed("boss", "boss_" + i)
				.withColumnRenamed("logEvents", "logEvents_" + i));
		}
		
		Column joinCond;
		joinCond = rows.get(0).col("subordonee_employeeID_0").equalTo(rows.get(1).col("subordonee_employeeID_1"));
		joinCond = joinCond.and(rows.get(0).col("boss_employeeID_0").equalTo(rows.get(1).col("boss_employeeID_1")));
		
		Dataset<Row> res = rows.get(0).join(rows.get(1), joinCond, joinMode);
		for(int i = 2; i < rows.size(); i++) {
			joinCond = rows.get(i - 1).col("subordonee_employeeID_" + (i - 1)).equalTo(rows.get(i).col("subordonee_employeeID_" + i));
			joinCond = joinCond.and(rows.get(i - 1).col("boss_employeeID_" + (i - 1)).equalTo(rows.get(i).col("boss_employeeID_" + i)));
			res = res.join(rows.get(i), joinCond, joinMode);
		}
	
		return res.map((MapFunction<Row, ReportsTo>) r -> {
				ReportsTo reportsTo_res = new ReportsTo();
	
					WrappedArray logEvents = r.getAs("logEvents_0");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							reportsTo_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							reportsTo_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					Employees subordonee_res = new Employees();
					Employees boss_res = new Employees();
					
					// attribute 'Employees.employeeID'
					Integer firstNotNull_subordonee_employeeID = Util.getIntegerValue(r.getAs("subordonee_0.employeeID"));
					subordonee_res.setEmployeeID(firstNotNull_subordonee_employeeID);
					// attribute 'Employees.lastName'
					String firstNotNull_subordonee_LastName = Util.getStringValue(r.getAs("subordonee_0.lastName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String subordonee_lastName2 = Util.getStringValue(r.getAs("subordonee_" + i + ".lastName"));
						if (firstNotNull_subordonee_LastName != null && subordonee_lastName2 != null && !firstNotNull_subordonee_LastName.equals(subordonee_lastName2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.lastName': " + firstNotNull_subordonee_LastName + " and " + subordonee_lastName2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.lastName': " + firstNotNull_subordonee_LastName + " and " + subordonee_lastName2 + "." );
						}
						if (firstNotNull_subordonee_LastName == null && subordonee_lastName2 != null) {
							firstNotNull_subordonee_LastName = subordonee_lastName2;
						}
					}
					subordonee_res.setLastName(firstNotNull_subordonee_LastName);
					// attribute 'Employees.firstName'
					String firstNotNull_subordonee_FirstName = Util.getStringValue(r.getAs("subordonee_0.firstName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String subordonee_firstName2 = Util.getStringValue(r.getAs("subordonee_" + i + ".firstName"));
						if (firstNotNull_subordonee_FirstName != null && subordonee_firstName2 != null && !firstNotNull_subordonee_FirstName.equals(subordonee_firstName2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.firstName': " + firstNotNull_subordonee_FirstName + " and " + subordonee_firstName2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.firstName': " + firstNotNull_subordonee_FirstName + " and " + subordonee_firstName2 + "." );
						}
						if (firstNotNull_subordonee_FirstName == null && subordonee_firstName2 != null) {
							firstNotNull_subordonee_FirstName = subordonee_firstName2;
						}
					}
					subordonee_res.setFirstName(firstNotNull_subordonee_FirstName);
					// attribute 'Employees.title'
					String firstNotNull_subordonee_Title = Util.getStringValue(r.getAs("subordonee_0.title"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String subordonee_title2 = Util.getStringValue(r.getAs("subordonee_" + i + ".title"));
						if (firstNotNull_subordonee_Title != null && subordonee_title2 != null && !firstNotNull_subordonee_Title.equals(subordonee_title2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.title': " + firstNotNull_subordonee_Title + " and " + subordonee_title2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.title': " + firstNotNull_subordonee_Title + " and " + subordonee_title2 + "." );
						}
						if (firstNotNull_subordonee_Title == null && subordonee_title2 != null) {
							firstNotNull_subordonee_Title = subordonee_title2;
						}
					}
					subordonee_res.setTitle(firstNotNull_subordonee_Title);
					// attribute 'Employees.titleOfCourtesy'
					String firstNotNull_subordonee_TitleOfCourtesy = Util.getStringValue(r.getAs("subordonee_0.titleOfCourtesy"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String subordonee_titleOfCourtesy2 = Util.getStringValue(r.getAs("subordonee_" + i + ".titleOfCourtesy"));
						if (firstNotNull_subordonee_TitleOfCourtesy != null && subordonee_titleOfCourtesy2 != null && !firstNotNull_subordonee_TitleOfCourtesy.equals(subordonee_titleOfCourtesy2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.titleOfCourtesy': " + firstNotNull_subordonee_TitleOfCourtesy + " and " + subordonee_titleOfCourtesy2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.titleOfCourtesy': " + firstNotNull_subordonee_TitleOfCourtesy + " and " + subordonee_titleOfCourtesy2 + "." );
						}
						if (firstNotNull_subordonee_TitleOfCourtesy == null && subordonee_titleOfCourtesy2 != null) {
							firstNotNull_subordonee_TitleOfCourtesy = subordonee_titleOfCourtesy2;
						}
					}
					subordonee_res.setTitleOfCourtesy(firstNotNull_subordonee_TitleOfCourtesy);
					// attribute 'Employees.birthDate'
					LocalDate firstNotNull_subordonee_BirthDate = Util.getLocalDateValue(r.getAs("subordonee_0.birthDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate subordonee_birthDate2 = Util.getLocalDateValue(r.getAs("subordonee_" + i + ".birthDate"));
						if (firstNotNull_subordonee_BirthDate != null && subordonee_birthDate2 != null && !firstNotNull_subordonee_BirthDate.equals(subordonee_birthDate2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.birthDate': " + firstNotNull_subordonee_BirthDate + " and " + subordonee_birthDate2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.birthDate': " + firstNotNull_subordonee_BirthDate + " and " + subordonee_birthDate2 + "." );
						}
						if (firstNotNull_subordonee_BirthDate == null && subordonee_birthDate2 != null) {
							firstNotNull_subordonee_BirthDate = subordonee_birthDate2;
						}
					}
					subordonee_res.setBirthDate(firstNotNull_subordonee_BirthDate);
					// attribute 'Employees.hireDate'
					LocalDate firstNotNull_subordonee_HireDate = Util.getLocalDateValue(r.getAs("subordonee_0.hireDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate subordonee_hireDate2 = Util.getLocalDateValue(r.getAs("subordonee_" + i + ".hireDate"));
						if (firstNotNull_subordonee_HireDate != null && subordonee_hireDate2 != null && !firstNotNull_subordonee_HireDate.equals(subordonee_hireDate2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.hireDate': " + firstNotNull_subordonee_HireDate + " and " + subordonee_hireDate2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.hireDate': " + firstNotNull_subordonee_HireDate + " and " + subordonee_hireDate2 + "." );
						}
						if (firstNotNull_subordonee_HireDate == null && subordonee_hireDate2 != null) {
							firstNotNull_subordonee_HireDate = subordonee_hireDate2;
						}
					}
					subordonee_res.setHireDate(firstNotNull_subordonee_HireDate);
					// attribute 'Employees.address'
					String firstNotNull_subordonee_Address = Util.getStringValue(r.getAs("subordonee_0.address"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String subordonee_address2 = Util.getStringValue(r.getAs("subordonee_" + i + ".address"));
						if (firstNotNull_subordonee_Address != null && subordonee_address2 != null && !firstNotNull_subordonee_Address.equals(subordonee_address2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.address': " + firstNotNull_subordonee_Address + " and " + subordonee_address2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.address': " + firstNotNull_subordonee_Address + " and " + subordonee_address2 + "." );
						}
						if (firstNotNull_subordonee_Address == null && subordonee_address2 != null) {
							firstNotNull_subordonee_Address = subordonee_address2;
						}
					}
					subordonee_res.setAddress(firstNotNull_subordonee_Address);
					// attribute 'Employees.city'
					String firstNotNull_subordonee_City = Util.getStringValue(r.getAs("subordonee_0.city"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String subordonee_city2 = Util.getStringValue(r.getAs("subordonee_" + i + ".city"));
						if (firstNotNull_subordonee_City != null && subordonee_city2 != null && !firstNotNull_subordonee_City.equals(subordonee_city2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.city': " + firstNotNull_subordonee_City + " and " + subordonee_city2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.city': " + firstNotNull_subordonee_City + " and " + subordonee_city2 + "." );
						}
						if (firstNotNull_subordonee_City == null && subordonee_city2 != null) {
							firstNotNull_subordonee_City = subordonee_city2;
						}
					}
					subordonee_res.setCity(firstNotNull_subordonee_City);
					// attribute 'Employees.region'
					String firstNotNull_subordonee_Region = Util.getStringValue(r.getAs("subordonee_0.region"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String subordonee_region2 = Util.getStringValue(r.getAs("subordonee_" + i + ".region"));
						if (firstNotNull_subordonee_Region != null && subordonee_region2 != null && !firstNotNull_subordonee_Region.equals(subordonee_region2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.region': " + firstNotNull_subordonee_Region + " and " + subordonee_region2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.region': " + firstNotNull_subordonee_Region + " and " + subordonee_region2 + "." );
						}
						if (firstNotNull_subordonee_Region == null && subordonee_region2 != null) {
							firstNotNull_subordonee_Region = subordonee_region2;
						}
					}
					subordonee_res.setRegion(firstNotNull_subordonee_Region);
					// attribute 'Employees.postalCode'
					String firstNotNull_subordonee_PostalCode = Util.getStringValue(r.getAs("subordonee_0.postalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String subordonee_postalCode2 = Util.getStringValue(r.getAs("subordonee_" + i + ".postalCode"));
						if (firstNotNull_subordonee_PostalCode != null && subordonee_postalCode2 != null && !firstNotNull_subordonee_PostalCode.equals(subordonee_postalCode2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.postalCode': " + firstNotNull_subordonee_PostalCode + " and " + subordonee_postalCode2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.postalCode': " + firstNotNull_subordonee_PostalCode + " and " + subordonee_postalCode2 + "." );
						}
						if (firstNotNull_subordonee_PostalCode == null && subordonee_postalCode2 != null) {
							firstNotNull_subordonee_PostalCode = subordonee_postalCode2;
						}
					}
					subordonee_res.setPostalCode(firstNotNull_subordonee_PostalCode);
					// attribute 'Employees.country'
					String firstNotNull_subordonee_Country = Util.getStringValue(r.getAs("subordonee_0.country"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String subordonee_country2 = Util.getStringValue(r.getAs("subordonee_" + i + ".country"));
						if (firstNotNull_subordonee_Country != null && subordonee_country2 != null && !firstNotNull_subordonee_Country.equals(subordonee_country2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.country': " + firstNotNull_subordonee_Country + " and " + subordonee_country2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.country': " + firstNotNull_subordonee_Country + " and " + subordonee_country2 + "." );
						}
						if (firstNotNull_subordonee_Country == null && subordonee_country2 != null) {
							firstNotNull_subordonee_Country = subordonee_country2;
						}
					}
					subordonee_res.setCountry(firstNotNull_subordonee_Country);
					// attribute 'Employees.homePhone'
					String firstNotNull_subordonee_HomePhone = Util.getStringValue(r.getAs("subordonee_0.homePhone"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String subordonee_homePhone2 = Util.getStringValue(r.getAs("subordonee_" + i + ".homePhone"));
						if (firstNotNull_subordonee_HomePhone != null && subordonee_homePhone2 != null && !firstNotNull_subordonee_HomePhone.equals(subordonee_homePhone2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.homePhone': " + firstNotNull_subordonee_HomePhone + " and " + subordonee_homePhone2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.homePhone': " + firstNotNull_subordonee_HomePhone + " and " + subordonee_homePhone2 + "." );
						}
						if (firstNotNull_subordonee_HomePhone == null && subordonee_homePhone2 != null) {
							firstNotNull_subordonee_HomePhone = subordonee_homePhone2;
						}
					}
					subordonee_res.setHomePhone(firstNotNull_subordonee_HomePhone);
					// attribute 'Employees.extension'
					String firstNotNull_subordonee_Extension = Util.getStringValue(r.getAs("subordonee_0.extension"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String subordonee_extension2 = Util.getStringValue(r.getAs("subordonee_" + i + ".extension"));
						if (firstNotNull_subordonee_Extension != null && subordonee_extension2 != null && !firstNotNull_subordonee_Extension.equals(subordonee_extension2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.extension': " + firstNotNull_subordonee_Extension + " and " + subordonee_extension2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.extension': " + firstNotNull_subordonee_Extension + " and " + subordonee_extension2 + "." );
						}
						if (firstNotNull_subordonee_Extension == null && subordonee_extension2 != null) {
							firstNotNull_subordonee_Extension = subordonee_extension2;
						}
					}
					subordonee_res.setExtension(firstNotNull_subordonee_Extension);
					// attribute 'Employees.photo'
					byte[] firstNotNull_subordonee_Photo = Util.getByteArrayValue(r.getAs("subordonee_0.photo"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						byte[] subordonee_photo2 = Util.getByteArrayValue(r.getAs("subordonee_" + i + ".photo"));
						if (firstNotNull_subordonee_Photo != null && subordonee_photo2 != null && !firstNotNull_subordonee_Photo.equals(subordonee_photo2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.photo': " + firstNotNull_subordonee_Photo + " and " + subordonee_photo2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.photo': " + firstNotNull_subordonee_Photo + " and " + subordonee_photo2 + "." );
						}
						if (firstNotNull_subordonee_Photo == null && subordonee_photo2 != null) {
							firstNotNull_subordonee_Photo = subordonee_photo2;
						}
					}
					subordonee_res.setPhoto(firstNotNull_subordonee_Photo);
					// attribute 'Employees.notes'
					String firstNotNull_subordonee_Notes = Util.getStringValue(r.getAs("subordonee_0.notes"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String subordonee_notes2 = Util.getStringValue(r.getAs("subordonee_" + i + ".notes"));
						if (firstNotNull_subordonee_Notes != null && subordonee_notes2 != null && !firstNotNull_subordonee_Notes.equals(subordonee_notes2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.notes': " + firstNotNull_subordonee_Notes + " and " + subordonee_notes2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.notes': " + firstNotNull_subordonee_Notes + " and " + subordonee_notes2 + "." );
						}
						if (firstNotNull_subordonee_Notes == null && subordonee_notes2 != null) {
							firstNotNull_subordonee_Notes = subordonee_notes2;
						}
					}
					subordonee_res.setNotes(firstNotNull_subordonee_Notes);
					// attribute 'Employees.photoPath'
					String firstNotNull_subordonee_PhotoPath = Util.getStringValue(r.getAs("subordonee_0.photoPath"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String subordonee_photoPath2 = Util.getStringValue(r.getAs("subordonee_" + i + ".photoPath"));
						if (firstNotNull_subordonee_PhotoPath != null && subordonee_photoPath2 != null && !firstNotNull_subordonee_PhotoPath.equals(subordonee_photoPath2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.photoPath': " + firstNotNull_subordonee_PhotoPath + " and " + subordonee_photoPath2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.photoPath': " + firstNotNull_subordonee_PhotoPath + " and " + subordonee_photoPath2 + "." );
						}
						if (firstNotNull_subordonee_PhotoPath == null && subordonee_photoPath2 != null) {
							firstNotNull_subordonee_PhotoPath = subordonee_photoPath2;
						}
					}
					subordonee_res.setPhotoPath(firstNotNull_subordonee_PhotoPath);
					// attribute 'Employees.salary'
					Double firstNotNull_subordonee_Salary = Util.getDoubleValue(r.getAs("subordonee_0.salary"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double subordonee_salary2 = Util.getDoubleValue(r.getAs("subordonee_" + i + ".salary"));
						if (firstNotNull_subordonee_Salary != null && subordonee_salary2 != null && !firstNotNull_subordonee_Salary.equals(subordonee_salary2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.salary': " + firstNotNull_subordonee_Salary + " and " + subordonee_salary2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+subordonee_res.getEmployeeID()+"]: different values found for attribute 'Employees.salary': " + firstNotNull_subordonee_Salary + " and " + subordonee_salary2 + "." );
						}
						if (firstNotNull_subordonee_Salary == null && subordonee_salary2 != null) {
							firstNotNull_subordonee_Salary = subordonee_salary2;
						}
					}
					subordonee_res.setSalary(firstNotNull_subordonee_Salary);
					// attribute 'Employees.employeeID'
					Integer firstNotNull_boss_employeeID = Util.getIntegerValue(r.getAs("boss_0.employeeID"));
					boss_res.setEmployeeID(firstNotNull_boss_employeeID);
					// attribute 'Employees.lastName'
					String firstNotNull_boss_LastName = Util.getStringValue(r.getAs("boss_0.lastName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String boss_lastName2 = Util.getStringValue(r.getAs("boss_" + i + ".lastName"));
						if (firstNotNull_boss_LastName != null && boss_lastName2 != null && !firstNotNull_boss_LastName.equals(boss_lastName2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.lastName': " + firstNotNull_boss_LastName + " and " + boss_lastName2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.lastName': " + firstNotNull_boss_LastName + " and " + boss_lastName2 + "." );
						}
						if (firstNotNull_boss_LastName == null && boss_lastName2 != null) {
							firstNotNull_boss_LastName = boss_lastName2;
						}
					}
					boss_res.setLastName(firstNotNull_boss_LastName);
					// attribute 'Employees.firstName'
					String firstNotNull_boss_FirstName = Util.getStringValue(r.getAs("boss_0.firstName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String boss_firstName2 = Util.getStringValue(r.getAs("boss_" + i + ".firstName"));
						if (firstNotNull_boss_FirstName != null && boss_firstName2 != null && !firstNotNull_boss_FirstName.equals(boss_firstName2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.firstName': " + firstNotNull_boss_FirstName + " and " + boss_firstName2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.firstName': " + firstNotNull_boss_FirstName + " and " + boss_firstName2 + "." );
						}
						if (firstNotNull_boss_FirstName == null && boss_firstName2 != null) {
							firstNotNull_boss_FirstName = boss_firstName2;
						}
					}
					boss_res.setFirstName(firstNotNull_boss_FirstName);
					// attribute 'Employees.title'
					String firstNotNull_boss_Title = Util.getStringValue(r.getAs("boss_0.title"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String boss_title2 = Util.getStringValue(r.getAs("boss_" + i + ".title"));
						if (firstNotNull_boss_Title != null && boss_title2 != null && !firstNotNull_boss_Title.equals(boss_title2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.title': " + firstNotNull_boss_Title + " and " + boss_title2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.title': " + firstNotNull_boss_Title + " and " + boss_title2 + "." );
						}
						if (firstNotNull_boss_Title == null && boss_title2 != null) {
							firstNotNull_boss_Title = boss_title2;
						}
					}
					boss_res.setTitle(firstNotNull_boss_Title);
					// attribute 'Employees.titleOfCourtesy'
					String firstNotNull_boss_TitleOfCourtesy = Util.getStringValue(r.getAs("boss_0.titleOfCourtesy"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String boss_titleOfCourtesy2 = Util.getStringValue(r.getAs("boss_" + i + ".titleOfCourtesy"));
						if (firstNotNull_boss_TitleOfCourtesy != null && boss_titleOfCourtesy2 != null && !firstNotNull_boss_TitleOfCourtesy.equals(boss_titleOfCourtesy2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.titleOfCourtesy': " + firstNotNull_boss_TitleOfCourtesy + " and " + boss_titleOfCourtesy2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.titleOfCourtesy': " + firstNotNull_boss_TitleOfCourtesy + " and " + boss_titleOfCourtesy2 + "." );
						}
						if (firstNotNull_boss_TitleOfCourtesy == null && boss_titleOfCourtesy2 != null) {
							firstNotNull_boss_TitleOfCourtesy = boss_titleOfCourtesy2;
						}
					}
					boss_res.setTitleOfCourtesy(firstNotNull_boss_TitleOfCourtesy);
					// attribute 'Employees.birthDate'
					LocalDate firstNotNull_boss_BirthDate = Util.getLocalDateValue(r.getAs("boss_0.birthDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate boss_birthDate2 = Util.getLocalDateValue(r.getAs("boss_" + i + ".birthDate"));
						if (firstNotNull_boss_BirthDate != null && boss_birthDate2 != null && !firstNotNull_boss_BirthDate.equals(boss_birthDate2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.birthDate': " + firstNotNull_boss_BirthDate + " and " + boss_birthDate2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.birthDate': " + firstNotNull_boss_BirthDate + " and " + boss_birthDate2 + "." );
						}
						if (firstNotNull_boss_BirthDate == null && boss_birthDate2 != null) {
							firstNotNull_boss_BirthDate = boss_birthDate2;
						}
					}
					boss_res.setBirthDate(firstNotNull_boss_BirthDate);
					// attribute 'Employees.hireDate'
					LocalDate firstNotNull_boss_HireDate = Util.getLocalDateValue(r.getAs("boss_0.hireDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate boss_hireDate2 = Util.getLocalDateValue(r.getAs("boss_" + i + ".hireDate"));
						if (firstNotNull_boss_HireDate != null && boss_hireDate2 != null && !firstNotNull_boss_HireDate.equals(boss_hireDate2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.hireDate': " + firstNotNull_boss_HireDate + " and " + boss_hireDate2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.hireDate': " + firstNotNull_boss_HireDate + " and " + boss_hireDate2 + "." );
						}
						if (firstNotNull_boss_HireDate == null && boss_hireDate2 != null) {
							firstNotNull_boss_HireDate = boss_hireDate2;
						}
					}
					boss_res.setHireDate(firstNotNull_boss_HireDate);
					// attribute 'Employees.address'
					String firstNotNull_boss_Address = Util.getStringValue(r.getAs("boss_0.address"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String boss_address2 = Util.getStringValue(r.getAs("boss_" + i + ".address"));
						if (firstNotNull_boss_Address != null && boss_address2 != null && !firstNotNull_boss_Address.equals(boss_address2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.address': " + firstNotNull_boss_Address + " and " + boss_address2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.address': " + firstNotNull_boss_Address + " and " + boss_address2 + "." );
						}
						if (firstNotNull_boss_Address == null && boss_address2 != null) {
							firstNotNull_boss_Address = boss_address2;
						}
					}
					boss_res.setAddress(firstNotNull_boss_Address);
					// attribute 'Employees.city'
					String firstNotNull_boss_City = Util.getStringValue(r.getAs("boss_0.city"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String boss_city2 = Util.getStringValue(r.getAs("boss_" + i + ".city"));
						if (firstNotNull_boss_City != null && boss_city2 != null && !firstNotNull_boss_City.equals(boss_city2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.city': " + firstNotNull_boss_City + " and " + boss_city2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.city': " + firstNotNull_boss_City + " and " + boss_city2 + "." );
						}
						if (firstNotNull_boss_City == null && boss_city2 != null) {
							firstNotNull_boss_City = boss_city2;
						}
					}
					boss_res.setCity(firstNotNull_boss_City);
					// attribute 'Employees.region'
					String firstNotNull_boss_Region = Util.getStringValue(r.getAs("boss_0.region"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String boss_region2 = Util.getStringValue(r.getAs("boss_" + i + ".region"));
						if (firstNotNull_boss_Region != null && boss_region2 != null && !firstNotNull_boss_Region.equals(boss_region2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.region': " + firstNotNull_boss_Region + " and " + boss_region2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.region': " + firstNotNull_boss_Region + " and " + boss_region2 + "." );
						}
						if (firstNotNull_boss_Region == null && boss_region2 != null) {
							firstNotNull_boss_Region = boss_region2;
						}
					}
					boss_res.setRegion(firstNotNull_boss_Region);
					// attribute 'Employees.postalCode'
					String firstNotNull_boss_PostalCode = Util.getStringValue(r.getAs("boss_0.postalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String boss_postalCode2 = Util.getStringValue(r.getAs("boss_" + i + ".postalCode"));
						if (firstNotNull_boss_PostalCode != null && boss_postalCode2 != null && !firstNotNull_boss_PostalCode.equals(boss_postalCode2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.postalCode': " + firstNotNull_boss_PostalCode + " and " + boss_postalCode2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.postalCode': " + firstNotNull_boss_PostalCode + " and " + boss_postalCode2 + "." );
						}
						if (firstNotNull_boss_PostalCode == null && boss_postalCode2 != null) {
							firstNotNull_boss_PostalCode = boss_postalCode2;
						}
					}
					boss_res.setPostalCode(firstNotNull_boss_PostalCode);
					// attribute 'Employees.country'
					String firstNotNull_boss_Country = Util.getStringValue(r.getAs("boss_0.country"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String boss_country2 = Util.getStringValue(r.getAs("boss_" + i + ".country"));
						if (firstNotNull_boss_Country != null && boss_country2 != null && !firstNotNull_boss_Country.equals(boss_country2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.country': " + firstNotNull_boss_Country + " and " + boss_country2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.country': " + firstNotNull_boss_Country + " and " + boss_country2 + "." );
						}
						if (firstNotNull_boss_Country == null && boss_country2 != null) {
							firstNotNull_boss_Country = boss_country2;
						}
					}
					boss_res.setCountry(firstNotNull_boss_Country);
					// attribute 'Employees.homePhone'
					String firstNotNull_boss_HomePhone = Util.getStringValue(r.getAs("boss_0.homePhone"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String boss_homePhone2 = Util.getStringValue(r.getAs("boss_" + i + ".homePhone"));
						if (firstNotNull_boss_HomePhone != null && boss_homePhone2 != null && !firstNotNull_boss_HomePhone.equals(boss_homePhone2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.homePhone': " + firstNotNull_boss_HomePhone + " and " + boss_homePhone2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.homePhone': " + firstNotNull_boss_HomePhone + " and " + boss_homePhone2 + "." );
						}
						if (firstNotNull_boss_HomePhone == null && boss_homePhone2 != null) {
							firstNotNull_boss_HomePhone = boss_homePhone2;
						}
					}
					boss_res.setHomePhone(firstNotNull_boss_HomePhone);
					// attribute 'Employees.extension'
					String firstNotNull_boss_Extension = Util.getStringValue(r.getAs("boss_0.extension"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String boss_extension2 = Util.getStringValue(r.getAs("boss_" + i + ".extension"));
						if (firstNotNull_boss_Extension != null && boss_extension2 != null && !firstNotNull_boss_Extension.equals(boss_extension2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.extension': " + firstNotNull_boss_Extension + " and " + boss_extension2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.extension': " + firstNotNull_boss_Extension + " and " + boss_extension2 + "." );
						}
						if (firstNotNull_boss_Extension == null && boss_extension2 != null) {
							firstNotNull_boss_Extension = boss_extension2;
						}
					}
					boss_res.setExtension(firstNotNull_boss_Extension);
					// attribute 'Employees.photo'
					byte[] firstNotNull_boss_Photo = Util.getByteArrayValue(r.getAs("boss_0.photo"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						byte[] boss_photo2 = Util.getByteArrayValue(r.getAs("boss_" + i + ".photo"));
						if (firstNotNull_boss_Photo != null && boss_photo2 != null && !firstNotNull_boss_Photo.equals(boss_photo2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.photo': " + firstNotNull_boss_Photo + " and " + boss_photo2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.photo': " + firstNotNull_boss_Photo + " and " + boss_photo2 + "." );
						}
						if (firstNotNull_boss_Photo == null && boss_photo2 != null) {
							firstNotNull_boss_Photo = boss_photo2;
						}
					}
					boss_res.setPhoto(firstNotNull_boss_Photo);
					// attribute 'Employees.notes'
					String firstNotNull_boss_Notes = Util.getStringValue(r.getAs("boss_0.notes"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String boss_notes2 = Util.getStringValue(r.getAs("boss_" + i + ".notes"));
						if (firstNotNull_boss_Notes != null && boss_notes2 != null && !firstNotNull_boss_Notes.equals(boss_notes2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.notes': " + firstNotNull_boss_Notes + " and " + boss_notes2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.notes': " + firstNotNull_boss_Notes + " and " + boss_notes2 + "." );
						}
						if (firstNotNull_boss_Notes == null && boss_notes2 != null) {
							firstNotNull_boss_Notes = boss_notes2;
						}
					}
					boss_res.setNotes(firstNotNull_boss_Notes);
					// attribute 'Employees.photoPath'
					String firstNotNull_boss_PhotoPath = Util.getStringValue(r.getAs("boss_0.photoPath"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String boss_photoPath2 = Util.getStringValue(r.getAs("boss_" + i + ".photoPath"));
						if (firstNotNull_boss_PhotoPath != null && boss_photoPath2 != null && !firstNotNull_boss_PhotoPath.equals(boss_photoPath2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.photoPath': " + firstNotNull_boss_PhotoPath + " and " + boss_photoPath2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.photoPath': " + firstNotNull_boss_PhotoPath + " and " + boss_photoPath2 + "." );
						}
						if (firstNotNull_boss_PhotoPath == null && boss_photoPath2 != null) {
							firstNotNull_boss_PhotoPath = boss_photoPath2;
						}
					}
					boss_res.setPhotoPath(firstNotNull_boss_PhotoPath);
					// attribute 'Employees.salary'
					Double firstNotNull_boss_Salary = Util.getDoubleValue(r.getAs("boss_0.salary"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double boss_salary2 = Util.getDoubleValue(r.getAs("boss_" + i + ".salary"));
						if (firstNotNull_boss_Salary != null && boss_salary2 != null && !firstNotNull_boss_Salary.equals(boss_salary2)) {
							reportsTo_res.addLogEvent("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.salary': " + firstNotNull_boss_Salary + " and " + boss_salary2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+boss_res.getEmployeeID()+"]: different values found for attribute 'Employees.salary': " + firstNotNull_boss_Salary + " and " + boss_salary2 + "." );
						}
						if (firstNotNull_boss_Salary == null && boss_salary2 != null) {
							firstNotNull_boss_Salary = boss_salary2;
						}
					}
					boss_res.setSalary(firstNotNull_boss_Salary);
	
					reportsTo_res.setSubordonee(subordonee_res);
					reportsTo_res.setBoss(boss_res);
					return reportsTo_res;
		}
		, Encoders.bean(ReportsTo.class));
	
	}
	
	//Empty arguments
	public Dataset<ReportsTo> getReportsToList(){
		 return getReportsToList(null,null);
	}
	
	public abstract Dataset<ReportsTo> getReportsToList(
		Condition<EmployeesAttribute> subordonee_condition,
		Condition<EmployeesAttribute> boss_condition);
	
	public Dataset<ReportsTo> getReportsToListBySubordoneeCondition(
		Condition<EmployeesAttribute> subordonee_condition
	){
		return getReportsToList(subordonee_condition, null);
	}
	
	public ReportsTo getReportsToBySubordonee(Employees subordonee) {
		Condition<EmployeesAttribute> cond = null;
		cond = Condition.simple(EmployeesAttribute.employeeID, Operator.EQUALS, subordonee.getEmployeeID());
		Dataset<ReportsTo> res = getReportsToListBySubordoneeCondition(cond);
		List<ReportsTo> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	public Dataset<ReportsTo> getReportsToListByBossCondition(
		Condition<EmployeesAttribute> boss_condition
	){
		return getReportsToList(null, boss_condition);
	}
	
	public Dataset<ReportsTo> getReportsToListByBoss(Employees boss) {
		Condition<EmployeesAttribute> cond = null;
		cond = Condition.simple(EmployeesAttribute.employeeID, Operator.EQUALS, boss.getEmployeeID());
		Dataset<ReportsTo> res = getReportsToListByBossCondition(cond);
	return res;
	}
	
	public abstract void insertReportsTo(ReportsTo reportsTo);
	
	
	
	public 	abstract boolean insertReportsToInRefStructEmployeesInMyRelDB(ReportsTo reportsTo);
	
	 public void insertReportsTo(Employees subordonee ,Employees boss ){
		ReportsTo reportsTo = new ReportsTo();
		reportsTo.setSubordonee(subordonee);
		reportsTo.setBoss(boss);
		insertReportsTo(reportsTo);
	}
	
	 public void insertReportsTo(Employees employees, List<Employees> bossList){
		ReportsTo reportsTo = new ReportsTo();
		reportsTo.setSubordonee(employees);
		for(Employees boss : bossList){
			reportsTo.setBoss(boss);
			insertReportsTo(reportsTo);
		}
	}
	
	
	public abstract void deleteReportsToList(
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition,
		conditions.Condition<conditions.EmployeesAttribute> boss_condition);
	
	public void deleteReportsToListBySubordoneeCondition(
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition
	){
		deleteReportsToList(subordonee_condition, null);
	}
	
	public void deleteReportsToBySubordonee(pojo.Employees subordonee) {
		// TODO using id for selecting
		return;
	}
	public void deleteReportsToListByBossCondition(
		conditions.Condition<conditions.EmployeesAttribute> boss_condition
	){
		deleteReportsToList(null, boss_condition);
	}
	
	public void deleteReportsToListByBoss(pojo.Employees boss) {
		// TODO using id for selecting
		return;
	}
		
}
