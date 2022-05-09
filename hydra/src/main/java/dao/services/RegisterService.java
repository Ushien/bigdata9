package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import conditions.*;
import pojo.Register;
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


public abstract class RegisterService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RegisterService.class);
	
	
	// Left side 'EmployeeRef' of reference [encoded ]
	public abstract Dataset<OrdersTDO> getOrdersTDOListProcessedOrderInEncodedInOrdersFromMongoDB(Condition<OrdersAttribute> condition, MutableBoolean refilterFlag);
	
	// Right side 'EmployeeID' of reference [encoded ]
	public abstract Dataset<EmployeesTDO> getEmployeesTDOListEmployeeInChargeInEncodedInOrdersFromMongoDB(Condition<EmployeesAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	public static Dataset<Register> fullLeftOuterJoinBetweenRegisterAndProcessedOrder(Dataset<Register> d1, Dataset<Orders> d2) {
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
		joinCond = d1.col("processedOrder.id").equalTo(d2_.col("A_id"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Register>) r -> {
				Register res = new Register();
	
				Orders processedOrder = new Orders();
				Object o = r.getAs("processedOrder");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						processedOrder.setId(Util.getIntegerValue(r2.getAs("id")));
						processedOrder.setOrderDate(Util.getLocalDateValue(r2.getAs("orderDate")));
						processedOrder.setRequiredDate(Util.getLocalDateValue(r2.getAs("requiredDate")));
						processedOrder.setShippedDate(Util.getLocalDateValue(r2.getAs("shippedDate")));
						processedOrder.setFreight(Util.getDoubleValue(r2.getAs("freight")));
						processedOrder.setShipName(Util.getStringValue(r2.getAs("shipName")));
						processedOrder.setShipAddress(Util.getStringValue(r2.getAs("shipAddress")));
						processedOrder.setShipCity(Util.getStringValue(r2.getAs("shipCity")));
						processedOrder.setShipRegion(Util.getStringValue(r2.getAs("shipRegion")));
						processedOrder.setShipPostalCode(Util.getStringValue(r2.getAs("shipPostalCode")));
						processedOrder.setShipCountry(Util.getStringValue(r2.getAs("shipCountry")));
					} 
					if(o instanceof Orders) {
						processedOrder = (Orders) o;
					}
				}
	
				res.setProcessedOrder(processedOrder);
	
				Integer id = Util.getIntegerValue(r.getAs("A_id"));
				if (processedOrder.getId() != null && id != null && !processedOrder.getId().equals(id)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.id': " + processedOrder.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.id': " + processedOrder.getId() + " and " + id + "." );
				}
				if(id != null)
					processedOrder.setId(id);
				LocalDate orderDate = Util.getLocalDateValue(r.getAs("A_orderDate"));
				if (processedOrder.getOrderDate() != null && orderDate != null && !processedOrder.getOrderDate().equals(orderDate)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.orderDate': " + processedOrder.getOrderDate() + " and " + orderDate + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.orderDate': " + processedOrder.getOrderDate() + " and " + orderDate + "." );
				}
				if(orderDate != null)
					processedOrder.setOrderDate(orderDate);
				LocalDate requiredDate = Util.getLocalDateValue(r.getAs("A_requiredDate"));
				if (processedOrder.getRequiredDate() != null && requiredDate != null && !processedOrder.getRequiredDate().equals(requiredDate)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.requiredDate': " + processedOrder.getRequiredDate() + " and " + requiredDate + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.requiredDate': " + processedOrder.getRequiredDate() + " and " + requiredDate + "." );
				}
				if(requiredDate != null)
					processedOrder.setRequiredDate(requiredDate);
				LocalDate shippedDate = Util.getLocalDateValue(r.getAs("A_shippedDate"));
				if (processedOrder.getShippedDate() != null && shippedDate != null && !processedOrder.getShippedDate().equals(shippedDate)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.shippedDate': " + processedOrder.getShippedDate() + " and " + shippedDate + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.shippedDate': " + processedOrder.getShippedDate() + " and " + shippedDate + "." );
				}
				if(shippedDate != null)
					processedOrder.setShippedDate(shippedDate);
				Double freight = Util.getDoubleValue(r.getAs("A_freight"));
				if (processedOrder.getFreight() != null && freight != null && !processedOrder.getFreight().equals(freight)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.freight': " + processedOrder.getFreight() + " and " + freight + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.freight': " + processedOrder.getFreight() + " and " + freight + "." );
				}
				if(freight != null)
					processedOrder.setFreight(freight);
				String shipName = Util.getStringValue(r.getAs("A_shipName"));
				if (processedOrder.getShipName() != null && shipName != null && !processedOrder.getShipName().equals(shipName)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.shipName': " + processedOrder.getShipName() + " and " + shipName + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.shipName': " + processedOrder.getShipName() + " and " + shipName + "." );
				}
				if(shipName != null)
					processedOrder.setShipName(shipName);
				String shipAddress = Util.getStringValue(r.getAs("A_shipAddress"));
				if (processedOrder.getShipAddress() != null && shipAddress != null && !processedOrder.getShipAddress().equals(shipAddress)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.shipAddress': " + processedOrder.getShipAddress() + " and " + shipAddress + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.shipAddress': " + processedOrder.getShipAddress() + " and " + shipAddress + "." );
				}
				if(shipAddress != null)
					processedOrder.setShipAddress(shipAddress);
				String shipCity = Util.getStringValue(r.getAs("A_shipCity"));
				if (processedOrder.getShipCity() != null && shipCity != null && !processedOrder.getShipCity().equals(shipCity)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.shipCity': " + processedOrder.getShipCity() + " and " + shipCity + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.shipCity': " + processedOrder.getShipCity() + " and " + shipCity + "." );
				}
				if(shipCity != null)
					processedOrder.setShipCity(shipCity);
				String shipRegion = Util.getStringValue(r.getAs("A_shipRegion"));
				if (processedOrder.getShipRegion() != null && shipRegion != null && !processedOrder.getShipRegion().equals(shipRegion)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.shipRegion': " + processedOrder.getShipRegion() + " and " + shipRegion + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.shipRegion': " + processedOrder.getShipRegion() + " and " + shipRegion + "." );
				}
				if(shipRegion != null)
					processedOrder.setShipRegion(shipRegion);
				String shipPostalCode = Util.getStringValue(r.getAs("A_shipPostalCode"));
				if (processedOrder.getShipPostalCode() != null && shipPostalCode != null && !processedOrder.getShipPostalCode().equals(shipPostalCode)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.shipPostalCode': " + processedOrder.getShipPostalCode() + " and " + shipPostalCode + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.shipPostalCode': " + processedOrder.getShipPostalCode() + " and " + shipPostalCode + "." );
				}
				if(shipPostalCode != null)
					processedOrder.setShipPostalCode(shipPostalCode);
				String shipCountry = Util.getStringValue(r.getAs("A_shipCountry"));
				if (processedOrder.getShipCountry() != null && shipCountry != null && !processedOrder.getShipCountry().equals(shipCountry)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.shipCountry': " + processedOrder.getShipCountry() + " and " + shipCountry + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.shipCountry': " + processedOrder.getShipCountry() + " and " + shipCountry + "." );
				}
				if(shipCountry != null)
					processedOrder.setShipCountry(shipCountry);
	
				o = r.getAs("employeeInCharge");
				Employees employeeInCharge = new Employees();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						employeeInCharge.setEmployeeID(Util.getIntegerValue(r2.getAs("employeeID")));
						employeeInCharge.setLastName(Util.getStringValue(r2.getAs("lastName")));
						employeeInCharge.setFirstName(Util.getStringValue(r2.getAs("firstName")));
						employeeInCharge.setTitle(Util.getStringValue(r2.getAs("title")));
						employeeInCharge.setTitleOfCourtesy(Util.getStringValue(r2.getAs("titleOfCourtesy")));
						employeeInCharge.setBirthDate(Util.getLocalDateValue(r2.getAs("birthDate")));
						employeeInCharge.setHireDate(Util.getLocalDateValue(r2.getAs("hireDate")));
						employeeInCharge.setAddress(Util.getStringValue(r2.getAs("address")));
						employeeInCharge.setCity(Util.getStringValue(r2.getAs("city")));
						employeeInCharge.setRegion(Util.getStringValue(r2.getAs("region")));
						employeeInCharge.setPostalCode(Util.getStringValue(r2.getAs("postalCode")));
						employeeInCharge.setCountry(Util.getStringValue(r2.getAs("country")));
						employeeInCharge.setHomePhone(Util.getStringValue(r2.getAs("homePhone")));
						employeeInCharge.setExtension(Util.getStringValue(r2.getAs("extension")));
						employeeInCharge.setPhoto(Util.getByteArrayValue(r2.getAs("photo")));
						employeeInCharge.setNotes(Util.getStringValue(r2.getAs("notes")));
						employeeInCharge.setPhotoPath(Util.getStringValue(r2.getAs("photoPath")));
						employeeInCharge.setSalary(Util.getDoubleValue(r2.getAs("salary")));
					} 
					if(o instanceof Employees) {
						employeeInCharge = (Employees) o;
					}
				}
	
				res.setEmployeeInCharge(employeeInCharge);
	
				return res;
		}, Encoders.bean(Register.class));
	
		
		
	}
	public static Dataset<Register> fullLeftOuterJoinBetweenRegisterAndEmployeeInCharge(Dataset<Register> d1, Dataset<Employees> d2) {
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
		joinCond = d1.col("employeeInCharge.employeeID").equalTo(d2_.col("A_employeeID"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Register>) r -> {
				Register res = new Register();
	
				Employees employeeInCharge = new Employees();
				Object o = r.getAs("employeeInCharge");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						employeeInCharge.setEmployeeID(Util.getIntegerValue(r2.getAs("employeeID")));
						employeeInCharge.setLastName(Util.getStringValue(r2.getAs("lastName")));
						employeeInCharge.setFirstName(Util.getStringValue(r2.getAs("firstName")));
						employeeInCharge.setTitle(Util.getStringValue(r2.getAs("title")));
						employeeInCharge.setTitleOfCourtesy(Util.getStringValue(r2.getAs("titleOfCourtesy")));
						employeeInCharge.setBirthDate(Util.getLocalDateValue(r2.getAs("birthDate")));
						employeeInCharge.setHireDate(Util.getLocalDateValue(r2.getAs("hireDate")));
						employeeInCharge.setAddress(Util.getStringValue(r2.getAs("address")));
						employeeInCharge.setCity(Util.getStringValue(r2.getAs("city")));
						employeeInCharge.setRegion(Util.getStringValue(r2.getAs("region")));
						employeeInCharge.setPostalCode(Util.getStringValue(r2.getAs("postalCode")));
						employeeInCharge.setCountry(Util.getStringValue(r2.getAs("country")));
						employeeInCharge.setHomePhone(Util.getStringValue(r2.getAs("homePhone")));
						employeeInCharge.setExtension(Util.getStringValue(r2.getAs("extension")));
						employeeInCharge.setPhoto(Util.getByteArrayValue(r2.getAs("photo")));
						employeeInCharge.setNotes(Util.getStringValue(r2.getAs("notes")));
						employeeInCharge.setPhotoPath(Util.getStringValue(r2.getAs("photoPath")));
						employeeInCharge.setSalary(Util.getDoubleValue(r2.getAs("salary")));
					} 
					if(o instanceof Employees) {
						employeeInCharge = (Employees) o;
					}
				}
	
				res.setEmployeeInCharge(employeeInCharge);
	
				Integer employeeID = Util.getIntegerValue(r.getAs("A_employeeID"));
				if (employeeInCharge.getEmployeeID() != null && employeeID != null && !employeeInCharge.getEmployeeID().equals(employeeID)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.employeeID': " + employeeInCharge.getEmployeeID() + " and " + employeeID + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.employeeID': " + employeeInCharge.getEmployeeID() + " and " + employeeID + "." );
				}
				if(employeeID != null)
					employeeInCharge.setEmployeeID(employeeID);
				String lastName = Util.getStringValue(r.getAs("A_lastName"));
				if (employeeInCharge.getLastName() != null && lastName != null && !employeeInCharge.getLastName().equals(lastName)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.lastName': " + employeeInCharge.getLastName() + " and " + lastName + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.lastName': " + employeeInCharge.getLastName() + " and " + lastName + "." );
				}
				if(lastName != null)
					employeeInCharge.setLastName(lastName);
				String firstName = Util.getStringValue(r.getAs("A_firstName"));
				if (employeeInCharge.getFirstName() != null && firstName != null && !employeeInCharge.getFirstName().equals(firstName)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.firstName': " + employeeInCharge.getFirstName() + " and " + firstName + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.firstName': " + employeeInCharge.getFirstName() + " and " + firstName + "." );
				}
				if(firstName != null)
					employeeInCharge.setFirstName(firstName);
				String title = Util.getStringValue(r.getAs("A_title"));
				if (employeeInCharge.getTitle() != null && title != null && !employeeInCharge.getTitle().equals(title)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.title': " + employeeInCharge.getTitle() + " and " + title + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.title': " + employeeInCharge.getTitle() + " and " + title + "." );
				}
				if(title != null)
					employeeInCharge.setTitle(title);
				String titleOfCourtesy = Util.getStringValue(r.getAs("A_titleOfCourtesy"));
				if (employeeInCharge.getTitleOfCourtesy() != null && titleOfCourtesy != null && !employeeInCharge.getTitleOfCourtesy().equals(titleOfCourtesy)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.titleOfCourtesy': " + employeeInCharge.getTitleOfCourtesy() + " and " + titleOfCourtesy + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.titleOfCourtesy': " + employeeInCharge.getTitleOfCourtesy() + " and " + titleOfCourtesy + "." );
				}
				if(titleOfCourtesy != null)
					employeeInCharge.setTitleOfCourtesy(titleOfCourtesy);
				LocalDate birthDate = Util.getLocalDateValue(r.getAs("A_birthDate"));
				if (employeeInCharge.getBirthDate() != null && birthDate != null && !employeeInCharge.getBirthDate().equals(birthDate)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.birthDate': " + employeeInCharge.getBirthDate() + " and " + birthDate + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.birthDate': " + employeeInCharge.getBirthDate() + " and " + birthDate + "." );
				}
				if(birthDate != null)
					employeeInCharge.setBirthDate(birthDate);
				LocalDate hireDate = Util.getLocalDateValue(r.getAs("A_hireDate"));
				if (employeeInCharge.getHireDate() != null && hireDate != null && !employeeInCharge.getHireDate().equals(hireDate)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.hireDate': " + employeeInCharge.getHireDate() + " and " + hireDate + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.hireDate': " + employeeInCharge.getHireDate() + " and " + hireDate + "." );
				}
				if(hireDate != null)
					employeeInCharge.setHireDate(hireDate);
				String address = Util.getStringValue(r.getAs("A_address"));
				if (employeeInCharge.getAddress() != null && address != null && !employeeInCharge.getAddress().equals(address)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.address': " + employeeInCharge.getAddress() + " and " + address + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.address': " + employeeInCharge.getAddress() + " and " + address + "." );
				}
				if(address != null)
					employeeInCharge.setAddress(address);
				String city = Util.getStringValue(r.getAs("A_city"));
				if (employeeInCharge.getCity() != null && city != null && !employeeInCharge.getCity().equals(city)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.city': " + employeeInCharge.getCity() + " and " + city + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.city': " + employeeInCharge.getCity() + " and " + city + "." );
				}
				if(city != null)
					employeeInCharge.setCity(city);
				String region = Util.getStringValue(r.getAs("A_region"));
				if (employeeInCharge.getRegion() != null && region != null && !employeeInCharge.getRegion().equals(region)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.region': " + employeeInCharge.getRegion() + " and " + region + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.region': " + employeeInCharge.getRegion() + " and " + region + "." );
				}
				if(region != null)
					employeeInCharge.setRegion(region);
				String postalCode = Util.getStringValue(r.getAs("A_postalCode"));
				if (employeeInCharge.getPostalCode() != null && postalCode != null && !employeeInCharge.getPostalCode().equals(postalCode)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.postalCode': " + employeeInCharge.getPostalCode() + " and " + postalCode + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.postalCode': " + employeeInCharge.getPostalCode() + " and " + postalCode + "." );
				}
				if(postalCode != null)
					employeeInCharge.setPostalCode(postalCode);
				String country = Util.getStringValue(r.getAs("A_country"));
				if (employeeInCharge.getCountry() != null && country != null && !employeeInCharge.getCountry().equals(country)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.country': " + employeeInCharge.getCountry() + " and " + country + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.country': " + employeeInCharge.getCountry() + " and " + country + "." );
				}
				if(country != null)
					employeeInCharge.setCountry(country);
				String homePhone = Util.getStringValue(r.getAs("A_homePhone"));
				if (employeeInCharge.getHomePhone() != null && homePhone != null && !employeeInCharge.getHomePhone().equals(homePhone)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.homePhone': " + employeeInCharge.getHomePhone() + " and " + homePhone + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.homePhone': " + employeeInCharge.getHomePhone() + " and " + homePhone + "." );
				}
				if(homePhone != null)
					employeeInCharge.setHomePhone(homePhone);
				String extension = Util.getStringValue(r.getAs("A_extension"));
				if (employeeInCharge.getExtension() != null && extension != null && !employeeInCharge.getExtension().equals(extension)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.extension': " + employeeInCharge.getExtension() + " and " + extension + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.extension': " + employeeInCharge.getExtension() + " and " + extension + "." );
				}
				if(extension != null)
					employeeInCharge.setExtension(extension);
				byte[] photo = Util.getByteArrayValue(r.getAs("A_photo"));
				if (employeeInCharge.getPhoto() != null && photo != null && !employeeInCharge.getPhoto().equals(photo)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.photo': " + employeeInCharge.getPhoto() + " and " + photo + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.photo': " + employeeInCharge.getPhoto() + " and " + photo + "." );
				}
				if(photo != null)
					employeeInCharge.setPhoto(photo);
				String notes = Util.getStringValue(r.getAs("A_notes"));
				if (employeeInCharge.getNotes() != null && notes != null && !employeeInCharge.getNotes().equals(notes)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.notes': " + employeeInCharge.getNotes() + " and " + notes + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.notes': " + employeeInCharge.getNotes() + " and " + notes + "." );
				}
				if(notes != null)
					employeeInCharge.setNotes(notes);
				String photoPath = Util.getStringValue(r.getAs("A_photoPath"));
				if (employeeInCharge.getPhotoPath() != null && photoPath != null && !employeeInCharge.getPhotoPath().equals(photoPath)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.photoPath': " + employeeInCharge.getPhotoPath() + " and " + photoPath + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.photoPath': " + employeeInCharge.getPhotoPath() + " and " + photoPath + "." );
				}
				if(photoPath != null)
					employeeInCharge.setPhotoPath(photoPath);
				Double salary = Util.getDoubleValue(r.getAs("A_salary"));
				if (employeeInCharge.getSalary() != null && salary != null && !employeeInCharge.getSalary().equals(salary)) {
					res.addLogEvent("Data consistency problem for [Register - different values found for attribute 'Register.salary': " + employeeInCharge.getSalary() + " and " + salary + "." );
					logger.warn("Data consistency problem for [Register - different values found for attribute 'Register.salary': " + employeeInCharge.getSalary() + " and " + salary + "." );
				}
				if(salary != null)
					employeeInCharge.setSalary(salary);
	
				o = r.getAs("processedOrder");
				Orders processedOrder = new Orders();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						processedOrder.setId(Util.getIntegerValue(r2.getAs("id")));
						processedOrder.setOrderDate(Util.getLocalDateValue(r2.getAs("orderDate")));
						processedOrder.setRequiredDate(Util.getLocalDateValue(r2.getAs("requiredDate")));
						processedOrder.setShippedDate(Util.getLocalDateValue(r2.getAs("shippedDate")));
						processedOrder.setFreight(Util.getDoubleValue(r2.getAs("freight")));
						processedOrder.setShipName(Util.getStringValue(r2.getAs("shipName")));
						processedOrder.setShipAddress(Util.getStringValue(r2.getAs("shipAddress")));
						processedOrder.setShipCity(Util.getStringValue(r2.getAs("shipCity")));
						processedOrder.setShipRegion(Util.getStringValue(r2.getAs("shipRegion")));
						processedOrder.setShipPostalCode(Util.getStringValue(r2.getAs("shipPostalCode")));
						processedOrder.setShipCountry(Util.getStringValue(r2.getAs("shipCountry")));
					} 
					if(o instanceof Orders) {
						processedOrder = (Orders) o;
					}
				}
	
				res.setProcessedOrder(processedOrder);
	
				return res;
		}, Encoders.bean(Register.class));
	
		
		
	}
	
	public static Dataset<Register> fullOuterJoinsRegister(List<Dataset<Register>> datasetsPOJO) {
		return fullOuterJoinsRegister(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Register> fullLeftOuterJoinsRegister(List<Dataset<Register>> datasetsPOJO) {
		return fullOuterJoinsRegister(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Register> fullOuterJoinsRegister(List<Dataset<Register>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		List<String> idFields = new ArrayList<String>();
		idFields.add("processedOrder.id");
	
		idFields.add("employeeInCharge.employeeID");
		scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
		
		List<Dataset<Row>> rows = new ArrayList<Dataset<Row>>();
		for(int i = 0; i < datasetsPOJO.size(); i++) {
			Dataset<Register> d = datasetsPOJO.get(i);
			rows.add(d
				.withColumn("processedOrder_id_" + i, d.col("processedOrder.id"))
				.withColumn("employeeInCharge_employeeID_" + i, d.col("employeeInCharge.employeeID"))
				.withColumnRenamed("processedOrder", "processedOrder_" + i)
				.withColumnRenamed("employeeInCharge", "employeeInCharge_" + i)
				.withColumnRenamed("logEvents", "logEvents_" + i));
		}
		
		Column joinCond;
		joinCond = rows.get(0).col("processedOrder_id_0").equalTo(rows.get(1).col("processedOrder_id_1"));
		joinCond = joinCond.and(rows.get(0).col("employeeInCharge_employeeID_0").equalTo(rows.get(1).col("employeeInCharge_employeeID_1")));
		
		Dataset<Row> res = rows.get(0).join(rows.get(1), joinCond, joinMode);
		for(int i = 2; i < rows.size(); i++) {
			joinCond = rows.get(i - 1).col("processedOrder_id_" + (i - 1)).equalTo(rows.get(i).col("processedOrder_id_" + i));
			joinCond = joinCond.and(rows.get(i - 1).col("employeeInCharge_employeeID_" + (i - 1)).equalTo(rows.get(i).col("employeeInCharge_employeeID_" + i)));
			res = res.join(rows.get(i), joinCond, joinMode);
		}
	
		return res.map((MapFunction<Row, Register>) r -> {
				Register register_res = new Register();
	
					WrappedArray logEvents = r.getAs("logEvents_0");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							register_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							register_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					Orders processedOrder_res = new Orders();
					Employees employeeInCharge_res = new Employees();
					
					// attribute 'Orders.id'
					Integer firstNotNull_processedOrder_id = Util.getIntegerValue(r.getAs("processedOrder_0.id"));
					processedOrder_res.setId(firstNotNull_processedOrder_id);
					// attribute 'Orders.orderDate'
					LocalDate firstNotNull_processedOrder_orderDate = Util.getLocalDateValue(r.getAs("processedOrder_0.orderDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate processedOrder_orderDate2 = Util.getLocalDateValue(r.getAs("processedOrder_" + i + ".orderDate"));
						if (firstNotNull_processedOrder_orderDate != null && processedOrder_orderDate2 != null && !firstNotNull_processedOrder_orderDate.equals(processedOrder_orderDate2)) {
							register_res.addLogEvent("Data consistency problem for [Orders - id :"+processedOrder_res.getId()+"]: different values found for attribute 'Orders.orderDate': " + firstNotNull_processedOrder_orderDate + " and " + processedOrder_orderDate2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+processedOrder_res.getId()+"]: different values found for attribute 'Orders.orderDate': " + firstNotNull_processedOrder_orderDate + " and " + processedOrder_orderDate2 + "." );
						}
						if (firstNotNull_processedOrder_orderDate == null && processedOrder_orderDate2 != null) {
							firstNotNull_processedOrder_orderDate = processedOrder_orderDate2;
						}
					}
					processedOrder_res.setOrderDate(firstNotNull_processedOrder_orderDate);
					// attribute 'Orders.requiredDate'
					LocalDate firstNotNull_processedOrder_requiredDate = Util.getLocalDateValue(r.getAs("processedOrder_0.requiredDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate processedOrder_requiredDate2 = Util.getLocalDateValue(r.getAs("processedOrder_" + i + ".requiredDate"));
						if (firstNotNull_processedOrder_requiredDate != null && processedOrder_requiredDate2 != null && !firstNotNull_processedOrder_requiredDate.equals(processedOrder_requiredDate2)) {
							register_res.addLogEvent("Data consistency problem for [Orders - id :"+processedOrder_res.getId()+"]: different values found for attribute 'Orders.requiredDate': " + firstNotNull_processedOrder_requiredDate + " and " + processedOrder_requiredDate2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+processedOrder_res.getId()+"]: different values found for attribute 'Orders.requiredDate': " + firstNotNull_processedOrder_requiredDate + " and " + processedOrder_requiredDate2 + "." );
						}
						if (firstNotNull_processedOrder_requiredDate == null && processedOrder_requiredDate2 != null) {
							firstNotNull_processedOrder_requiredDate = processedOrder_requiredDate2;
						}
					}
					processedOrder_res.setRequiredDate(firstNotNull_processedOrder_requiredDate);
					// attribute 'Orders.shippedDate'
					LocalDate firstNotNull_processedOrder_shippedDate = Util.getLocalDateValue(r.getAs("processedOrder_0.shippedDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate processedOrder_shippedDate2 = Util.getLocalDateValue(r.getAs("processedOrder_" + i + ".shippedDate"));
						if (firstNotNull_processedOrder_shippedDate != null && processedOrder_shippedDate2 != null && !firstNotNull_processedOrder_shippedDate.equals(processedOrder_shippedDate2)) {
							register_res.addLogEvent("Data consistency problem for [Orders - id :"+processedOrder_res.getId()+"]: different values found for attribute 'Orders.shippedDate': " + firstNotNull_processedOrder_shippedDate + " and " + processedOrder_shippedDate2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+processedOrder_res.getId()+"]: different values found for attribute 'Orders.shippedDate': " + firstNotNull_processedOrder_shippedDate + " and " + processedOrder_shippedDate2 + "." );
						}
						if (firstNotNull_processedOrder_shippedDate == null && processedOrder_shippedDate2 != null) {
							firstNotNull_processedOrder_shippedDate = processedOrder_shippedDate2;
						}
					}
					processedOrder_res.setShippedDate(firstNotNull_processedOrder_shippedDate);
					// attribute 'Orders.freight'
					Double firstNotNull_processedOrder_freight = Util.getDoubleValue(r.getAs("processedOrder_0.freight"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double processedOrder_freight2 = Util.getDoubleValue(r.getAs("processedOrder_" + i + ".freight"));
						if (firstNotNull_processedOrder_freight != null && processedOrder_freight2 != null && !firstNotNull_processedOrder_freight.equals(processedOrder_freight2)) {
							register_res.addLogEvent("Data consistency problem for [Orders - id :"+processedOrder_res.getId()+"]: different values found for attribute 'Orders.freight': " + firstNotNull_processedOrder_freight + " and " + processedOrder_freight2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+processedOrder_res.getId()+"]: different values found for attribute 'Orders.freight': " + firstNotNull_processedOrder_freight + " and " + processedOrder_freight2 + "." );
						}
						if (firstNotNull_processedOrder_freight == null && processedOrder_freight2 != null) {
							firstNotNull_processedOrder_freight = processedOrder_freight2;
						}
					}
					processedOrder_res.setFreight(firstNotNull_processedOrder_freight);
					// attribute 'Orders.shipName'
					String firstNotNull_processedOrder_shipName = Util.getStringValue(r.getAs("processedOrder_0.shipName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String processedOrder_shipName2 = Util.getStringValue(r.getAs("processedOrder_" + i + ".shipName"));
						if (firstNotNull_processedOrder_shipName != null && processedOrder_shipName2 != null && !firstNotNull_processedOrder_shipName.equals(processedOrder_shipName2)) {
							register_res.addLogEvent("Data consistency problem for [Orders - id :"+processedOrder_res.getId()+"]: different values found for attribute 'Orders.shipName': " + firstNotNull_processedOrder_shipName + " and " + processedOrder_shipName2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+processedOrder_res.getId()+"]: different values found for attribute 'Orders.shipName': " + firstNotNull_processedOrder_shipName + " and " + processedOrder_shipName2 + "." );
						}
						if (firstNotNull_processedOrder_shipName == null && processedOrder_shipName2 != null) {
							firstNotNull_processedOrder_shipName = processedOrder_shipName2;
						}
					}
					processedOrder_res.setShipName(firstNotNull_processedOrder_shipName);
					// attribute 'Orders.shipAddress'
					String firstNotNull_processedOrder_shipAddress = Util.getStringValue(r.getAs("processedOrder_0.shipAddress"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String processedOrder_shipAddress2 = Util.getStringValue(r.getAs("processedOrder_" + i + ".shipAddress"));
						if (firstNotNull_processedOrder_shipAddress != null && processedOrder_shipAddress2 != null && !firstNotNull_processedOrder_shipAddress.equals(processedOrder_shipAddress2)) {
							register_res.addLogEvent("Data consistency problem for [Orders - id :"+processedOrder_res.getId()+"]: different values found for attribute 'Orders.shipAddress': " + firstNotNull_processedOrder_shipAddress + " and " + processedOrder_shipAddress2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+processedOrder_res.getId()+"]: different values found for attribute 'Orders.shipAddress': " + firstNotNull_processedOrder_shipAddress + " and " + processedOrder_shipAddress2 + "." );
						}
						if (firstNotNull_processedOrder_shipAddress == null && processedOrder_shipAddress2 != null) {
							firstNotNull_processedOrder_shipAddress = processedOrder_shipAddress2;
						}
					}
					processedOrder_res.setShipAddress(firstNotNull_processedOrder_shipAddress);
					// attribute 'Orders.shipCity'
					String firstNotNull_processedOrder_shipCity = Util.getStringValue(r.getAs("processedOrder_0.shipCity"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String processedOrder_shipCity2 = Util.getStringValue(r.getAs("processedOrder_" + i + ".shipCity"));
						if (firstNotNull_processedOrder_shipCity != null && processedOrder_shipCity2 != null && !firstNotNull_processedOrder_shipCity.equals(processedOrder_shipCity2)) {
							register_res.addLogEvent("Data consistency problem for [Orders - id :"+processedOrder_res.getId()+"]: different values found for attribute 'Orders.shipCity': " + firstNotNull_processedOrder_shipCity + " and " + processedOrder_shipCity2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+processedOrder_res.getId()+"]: different values found for attribute 'Orders.shipCity': " + firstNotNull_processedOrder_shipCity + " and " + processedOrder_shipCity2 + "." );
						}
						if (firstNotNull_processedOrder_shipCity == null && processedOrder_shipCity2 != null) {
							firstNotNull_processedOrder_shipCity = processedOrder_shipCity2;
						}
					}
					processedOrder_res.setShipCity(firstNotNull_processedOrder_shipCity);
					// attribute 'Orders.shipRegion'
					String firstNotNull_processedOrder_shipRegion = Util.getStringValue(r.getAs("processedOrder_0.shipRegion"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String processedOrder_shipRegion2 = Util.getStringValue(r.getAs("processedOrder_" + i + ".shipRegion"));
						if (firstNotNull_processedOrder_shipRegion != null && processedOrder_shipRegion2 != null && !firstNotNull_processedOrder_shipRegion.equals(processedOrder_shipRegion2)) {
							register_res.addLogEvent("Data consistency problem for [Orders - id :"+processedOrder_res.getId()+"]: different values found for attribute 'Orders.shipRegion': " + firstNotNull_processedOrder_shipRegion + " and " + processedOrder_shipRegion2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+processedOrder_res.getId()+"]: different values found for attribute 'Orders.shipRegion': " + firstNotNull_processedOrder_shipRegion + " and " + processedOrder_shipRegion2 + "." );
						}
						if (firstNotNull_processedOrder_shipRegion == null && processedOrder_shipRegion2 != null) {
							firstNotNull_processedOrder_shipRegion = processedOrder_shipRegion2;
						}
					}
					processedOrder_res.setShipRegion(firstNotNull_processedOrder_shipRegion);
					// attribute 'Orders.shipPostalCode'
					String firstNotNull_processedOrder_shipPostalCode = Util.getStringValue(r.getAs("processedOrder_0.shipPostalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String processedOrder_shipPostalCode2 = Util.getStringValue(r.getAs("processedOrder_" + i + ".shipPostalCode"));
						if (firstNotNull_processedOrder_shipPostalCode != null && processedOrder_shipPostalCode2 != null && !firstNotNull_processedOrder_shipPostalCode.equals(processedOrder_shipPostalCode2)) {
							register_res.addLogEvent("Data consistency problem for [Orders - id :"+processedOrder_res.getId()+"]: different values found for attribute 'Orders.shipPostalCode': " + firstNotNull_processedOrder_shipPostalCode + " and " + processedOrder_shipPostalCode2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+processedOrder_res.getId()+"]: different values found for attribute 'Orders.shipPostalCode': " + firstNotNull_processedOrder_shipPostalCode + " and " + processedOrder_shipPostalCode2 + "." );
						}
						if (firstNotNull_processedOrder_shipPostalCode == null && processedOrder_shipPostalCode2 != null) {
							firstNotNull_processedOrder_shipPostalCode = processedOrder_shipPostalCode2;
						}
					}
					processedOrder_res.setShipPostalCode(firstNotNull_processedOrder_shipPostalCode);
					// attribute 'Orders.shipCountry'
					String firstNotNull_processedOrder_shipCountry = Util.getStringValue(r.getAs("processedOrder_0.shipCountry"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String processedOrder_shipCountry2 = Util.getStringValue(r.getAs("processedOrder_" + i + ".shipCountry"));
						if (firstNotNull_processedOrder_shipCountry != null && processedOrder_shipCountry2 != null && !firstNotNull_processedOrder_shipCountry.equals(processedOrder_shipCountry2)) {
							register_res.addLogEvent("Data consistency problem for [Orders - id :"+processedOrder_res.getId()+"]: different values found for attribute 'Orders.shipCountry': " + firstNotNull_processedOrder_shipCountry + " and " + processedOrder_shipCountry2 + "." );
							logger.warn("Data consistency problem for [Orders - id :"+processedOrder_res.getId()+"]: different values found for attribute 'Orders.shipCountry': " + firstNotNull_processedOrder_shipCountry + " and " + processedOrder_shipCountry2 + "." );
						}
						if (firstNotNull_processedOrder_shipCountry == null && processedOrder_shipCountry2 != null) {
							firstNotNull_processedOrder_shipCountry = processedOrder_shipCountry2;
						}
					}
					processedOrder_res.setShipCountry(firstNotNull_processedOrder_shipCountry);
					// attribute 'Employees.employeeID'
					Integer firstNotNull_employeeInCharge_employeeID = Util.getIntegerValue(r.getAs("employeeInCharge_0.employeeID"));
					employeeInCharge_res.setEmployeeID(firstNotNull_employeeInCharge_employeeID);
					// attribute 'Employees.lastName'
					String firstNotNull_employeeInCharge_lastName = Util.getStringValue(r.getAs("employeeInCharge_0.lastName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employeeInCharge_lastName2 = Util.getStringValue(r.getAs("employeeInCharge_" + i + ".lastName"));
						if (firstNotNull_employeeInCharge_lastName != null && employeeInCharge_lastName2 != null && !firstNotNull_employeeInCharge_lastName.equals(employeeInCharge_lastName2)) {
							register_res.addLogEvent("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.lastName': " + firstNotNull_employeeInCharge_lastName + " and " + employeeInCharge_lastName2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.lastName': " + firstNotNull_employeeInCharge_lastName + " and " + employeeInCharge_lastName2 + "." );
						}
						if (firstNotNull_employeeInCharge_lastName == null && employeeInCharge_lastName2 != null) {
							firstNotNull_employeeInCharge_lastName = employeeInCharge_lastName2;
						}
					}
					employeeInCharge_res.setLastName(firstNotNull_employeeInCharge_lastName);
					// attribute 'Employees.firstName'
					String firstNotNull_employeeInCharge_firstName = Util.getStringValue(r.getAs("employeeInCharge_0.firstName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employeeInCharge_firstName2 = Util.getStringValue(r.getAs("employeeInCharge_" + i + ".firstName"));
						if (firstNotNull_employeeInCharge_firstName != null && employeeInCharge_firstName2 != null && !firstNotNull_employeeInCharge_firstName.equals(employeeInCharge_firstName2)) {
							register_res.addLogEvent("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.firstName': " + firstNotNull_employeeInCharge_firstName + " and " + employeeInCharge_firstName2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.firstName': " + firstNotNull_employeeInCharge_firstName + " and " + employeeInCharge_firstName2 + "." );
						}
						if (firstNotNull_employeeInCharge_firstName == null && employeeInCharge_firstName2 != null) {
							firstNotNull_employeeInCharge_firstName = employeeInCharge_firstName2;
						}
					}
					employeeInCharge_res.setFirstName(firstNotNull_employeeInCharge_firstName);
					// attribute 'Employees.title'
					String firstNotNull_employeeInCharge_title = Util.getStringValue(r.getAs("employeeInCharge_0.title"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employeeInCharge_title2 = Util.getStringValue(r.getAs("employeeInCharge_" + i + ".title"));
						if (firstNotNull_employeeInCharge_title != null && employeeInCharge_title2 != null && !firstNotNull_employeeInCharge_title.equals(employeeInCharge_title2)) {
							register_res.addLogEvent("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.title': " + firstNotNull_employeeInCharge_title + " and " + employeeInCharge_title2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.title': " + firstNotNull_employeeInCharge_title + " and " + employeeInCharge_title2 + "." );
						}
						if (firstNotNull_employeeInCharge_title == null && employeeInCharge_title2 != null) {
							firstNotNull_employeeInCharge_title = employeeInCharge_title2;
						}
					}
					employeeInCharge_res.setTitle(firstNotNull_employeeInCharge_title);
					// attribute 'Employees.titleOfCourtesy'
					String firstNotNull_employeeInCharge_titleOfCourtesy = Util.getStringValue(r.getAs("employeeInCharge_0.titleOfCourtesy"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employeeInCharge_titleOfCourtesy2 = Util.getStringValue(r.getAs("employeeInCharge_" + i + ".titleOfCourtesy"));
						if (firstNotNull_employeeInCharge_titleOfCourtesy != null && employeeInCharge_titleOfCourtesy2 != null && !firstNotNull_employeeInCharge_titleOfCourtesy.equals(employeeInCharge_titleOfCourtesy2)) {
							register_res.addLogEvent("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.titleOfCourtesy': " + firstNotNull_employeeInCharge_titleOfCourtesy + " and " + employeeInCharge_titleOfCourtesy2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.titleOfCourtesy': " + firstNotNull_employeeInCharge_titleOfCourtesy + " and " + employeeInCharge_titleOfCourtesy2 + "." );
						}
						if (firstNotNull_employeeInCharge_titleOfCourtesy == null && employeeInCharge_titleOfCourtesy2 != null) {
							firstNotNull_employeeInCharge_titleOfCourtesy = employeeInCharge_titleOfCourtesy2;
						}
					}
					employeeInCharge_res.setTitleOfCourtesy(firstNotNull_employeeInCharge_titleOfCourtesy);
					// attribute 'Employees.birthDate'
					LocalDate firstNotNull_employeeInCharge_birthDate = Util.getLocalDateValue(r.getAs("employeeInCharge_0.birthDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate employeeInCharge_birthDate2 = Util.getLocalDateValue(r.getAs("employeeInCharge_" + i + ".birthDate"));
						if (firstNotNull_employeeInCharge_birthDate != null && employeeInCharge_birthDate2 != null && !firstNotNull_employeeInCharge_birthDate.equals(employeeInCharge_birthDate2)) {
							register_res.addLogEvent("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.birthDate': " + firstNotNull_employeeInCharge_birthDate + " and " + employeeInCharge_birthDate2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.birthDate': " + firstNotNull_employeeInCharge_birthDate + " and " + employeeInCharge_birthDate2 + "." );
						}
						if (firstNotNull_employeeInCharge_birthDate == null && employeeInCharge_birthDate2 != null) {
							firstNotNull_employeeInCharge_birthDate = employeeInCharge_birthDate2;
						}
					}
					employeeInCharge_res.setBirthDate(firstNotNull_employeeInCharge_birthDate);
					// attribute 'Employees.hireDate'
					LocalDate firstNotNull_employeeInCharge_hireDate = Util.getLocalDateValue(r.getAs("employeeInCharge_0.hireDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate employeeInCharge_hireDate2 = Util.getLocalDateValue(r.getAs("employeeInCharge_" + i + ".hireDate"));
						if (firstNotNull_employeeInCharge_hireDate != null && employeeInCharge_hireDate2 != null && !firstNotNull_employeeInCharge_hireDate.equals(employeeInCharge_hireDate2)) {
							register_res.addLogEvent("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.hireDate': " + firstNotNull_employeeInCharge_hireDate + " and " + employeeInCharge_hireDate2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.hireDate': " + firstNotNull_employeeInCharge_hireDate + " and " + employeeInCharge_hireDate2 + "." );
						}
						if (firstNotNull_employeeInCharge_hireDate == null && employeeInCharge_hireDate2 != null) {
							firstNotNull_employeeInCharge_hireDate = employeeInCharge_hireDate2;
						}
					}
					employeeInCharge_res.setHireDate(firstNotNull_employeeInCharge_hireDate);
					// attribute 'Employees.address'
					String firstNotNull_employeeInCharge_address = Util.getStringValue(r.getAs("employeeInCharge_0.address"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employeeInCharge_address2 = Util.getStringValue(r.getAs("employeeInCharge_" + i + ".address"));
						if (firstNotNull_employeeInCharge_address != null && employeeInCharge_address2 != null && !firstNotNull_employeeInCharge_address.equals(employeeInCharge_address2)) {
							register_res.addLogEvent("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.address': " + firstNotNull_employeeInCharge_address + " and " + employeeInCharge_address2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.address': " + firstNotNull_employeeInCharge_address + " and " + employeeInCharge_address2 + "." );
						}
						if (firstNotNull_employeeInCharge_address == null && employeeInCharge_address2 != null) {
							firstNotNull_employeeInCharge_address = employeeInCharge_address2;
						}
					}
					employeeInCharge_res.setAddress(firstNotNull_employeeInCharge_address);
					// attribute 'Employees.city'
					String firstNotNull_employeeInCharge_city = Util.getStringValue(r.getAs("employeeInCharge_0.city"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employeeInCharge_city2 = Util.getStringValue(r.getAs("employeeInCharge_" + i + ".city"));
						if (firstNotNull_employeeInCharge_city != null && employeeInCharge_city2 != null && !firstNotNull_employeeInCharge_city.equals(employeeInCharge_city2)) {
							register_res.addLogEvent("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.city': " + firstNotNull_employeeInCharge_city + " and " + employeeInCharge_city2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.city': " + firstNotNull_employeeInCharge_city + " and " + employeeInCharge_city2 + "." );
						}
						if (firstNotNull_employeeInCharge_city == null && employeeInCharge_city2 != null) {
							firstNotNull_employeeInCharge_city = employeeInCharge_city2;
						}
					}
					employeeInCharge_res.setCity(firstNotNull_employeeInCharge_city);
					// attribute 'Employees.region'
					String firstNotNull_employeeInCharge_region = Util.getStringValue(r.getAs("employeeInCharge_0.region"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employeeInCharge_region2 = Util.getStringValue(r.getAs("employeeInCharge_" + i + ".region"));
						if (firstNotNull_employeeInCharge_region != null && employeeInCharge_region2 != null && !firstNotNull_employeeInCharge_region.equals(employeeInCharge_region2)) {
							register_res.addLogEvent("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.region': " + firstNotNull_employeeInCharge_region + " and " + employeeInCharge_region2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.region': " + firstNotNull_employeeInCharge_region + " and " + employeeInCharge_region2 + "." );
						}
						if (firstNotNull_employeeInCharge_region == null && employeeInCharge_region2 != null) {
							firstNotNull_employeeInCharge_region = employeeInCharge_region2;
						}
					}
					employeeInCharge_res.setRegion(firstNotNull_employeeInCharge_region);
					// attribute 'Employees.postalCode'
					String firstNotNull_employeeInCharge_postalCode = Util.getStringValue(r.getAs("employeeInCharge_0.postalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employeeInCharge_postalCode2 = Util.getStringValue(r.getAs("employeeInCharge_" + i + ".postalCode"));
						if (firstNotNull_employeeInCharge_postalCode != null && employeeInCharge_postalCode2 != null && !firstNotNull_employeeInCharge_postalCode.equals(employeeInCharge_postalCode2)) {
							register_res.addLogEvent("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.postalCode': " + firstNotNull_employeeInCharge_postalCode + " and " + employeeInCharge_postalCode2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.postalCode': " + firstNotNull_employeeInCharge_postalCode + " and " + employeeInCharge_postalCode2 + "." );
						}
						if (firstNotNull_employeeInCharge_postalCode == null && employeeInCharge_postalCode2 != null) {
							firstNotNull_employeeInCharge_postalCode = employeeInCharge_postalCode2;
						}
					}
					employeeInCharge_res.setPostalCode(firstNotNull_employeeInCharge_postalCode);
					// attribute 'Employees.country'
					String firstNotNull_employeeInCharge_country = Util.getStringValue(r.getAs("employeeInCharge_0.country"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employeeInCharge_country2 = Util.getStringValue(r.getAs("employeeInCharge_" + i + ".country"));
						if (firstNotNull_employeeInCharge_country != null && employeeInCharge_country2 != null && !firstNotNull_employeeInCharge_country.equals(employeeInCharge_country2)) {
							register_res.addLogEvent("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.country': " + firstNotNull_employeeInCharge_country + " and " + employeeInCharge_country2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.country': " + firstNotNull_employeeInCharge_country + " and " + employeeInCharge_country2 + "." );
						}
						if (firstNotNull_employeeInCharge_country == null && employeeInCharge_country2 != null) {
							firstNotNull_employeeInCharge_country = employeeInCharge_country2;
						}
					}
					employeeInCharge_res.setCountry(firstNotNull_employeeInCharge_country);
					// attribute 'Employees.homePhone'
					String firstNotNull_employeeInCharge_homePhone = Util.getStringValue(r.getAs("employeeInCharge_0.homePhone"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employeeInCharge_homePhone2 = Util.getStringValue(r.getAs("employeeInCharge_" + i + ".homePhone"));
						if (firstNotNull_employeeInCharge_homePhone != null && employeeInCharge_homePhone2 != null && !firstNotNull_employeeInCharge_homePhone.equals(employeeInCharge_homePhone2)) {
							register_res.addLogEvent("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.homePhone': " + firstNotNull_employeeInCharge_homePhone + " and " + employeeInCharge_homePhone2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.homePhone': " + firstNotNull_employeeInCharge_homePhone + " and " + employeeInCharge_homePhone2 + "." );
						}
						if (firstNotNull_employeeInCharge_homePhone == null && employeeInCharge_homePhone2 != null) {
							firstNotNull_employeeInCharge_homePhone = employeeInCharge_homePhone2;
						}
					}
					employeeInCharge_res.setHomePhone(firstNotNull_employeeInCharge_homePhone);
					// attribute 'Employees.extension'
					String firstNotNull_employeeInCharge_extension = Util.getStringValue(r.getAs("employeeInCharge_0.extension"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employeeInCharge_extension2 = Util.getStringValue(r.getAs("employeeInCharge_" + i + ".extension"));
						if (firstNotNull_employeeInCharge_extension != null && employeeInCharge_extension2 != null && !firstNotNull_employeeInCharge_extension.equals(employeeInCharge_extension2)) {
							register_res.addLogEvent("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.extension': " + firstNotNull_employeeInCharge_extension + " and " + employeeInCharge_extension2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.extension': " + firstNotNull_employeeInCharge_extension + " and " + employeeInCharge_extension2 + "." );
						}
						if (firstNotNull_employeeInCharge_extension == null && employeeInCharge_extension2 != null) {
							firstNotNull_employeeInCharge_extension = employeeInCharge_extension2;
						}
					}
					employeeInCharge_res.setExtension(firstNotNull_employeeInCharge_extension);
					// attribute 'Employees.photo'
					byte[] firstNotNull_employeeInCharge_photo = Util.getByteArrayValue(r.getAs("employeeInCharge_0.photo"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						byte[] employeeInCharge_photo2 = Util.getByteArrayValue(r.getAs("employeeInCharge_" + i + ".photo"));
						if (firstNotNull_employeeInCharge_photo != null && employeeInCharge_photo2 != null && !firstNotNull_employeeInCharge_photo.equals(employeeInCharge_photo2)) {
							register_res.addLogEvent("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.photo': " + firstNotNull_employeeInCharge_photo + " and " + employeeInCharge_photo2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.photo': " + firstNotNull_employeeInCharge_photo + " and " + employeeInCharge_photo2 + "." );
						}
						if (firstNotNull_employeeInCharge_photo == null && employeeInCharge_photo2 != null) {
							firstNotNull_employeeInCharge_photo = employeeInCharge_photo2;
						}
					}
					employeeInCharge_res.setPhoto(firstNotNull_employeeInCharge_photo);
					// attribute 'Employees.notes'
					String firstNotNull_employeeInCharge_notes = Util.getStringValue(r.getAs("employeeInCharge_0.notes"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employeeInCharge_notes2 = Util.getStringValue(r.getAs("employeeInCharge_" + i + ".notes"));
						if (firstNotNull_employeeInCharge_notes != null && employeeInCharge_notes2 != null && !firstNotNull_employeeInCharge_notes.equals(employeeInCharge_notes2)) {
							register_res.addLogEvent("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.notes': " + firstNotNull_employeeInCharge_notes + " and " + employeeInCharge_notes2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.notes': " + firstNotNull_employeeInCharge_notes + " and " + employeeInCharge_notes2 + "." );
						}
						if (firstNotNull_employeeInCharge_notes == null && employeeInCharge_notes2 != null) {
							firstNotNull_employeeInCharge_notes = employeeInCharge_notes2;
						}
					}
					employeeInCharge_res.setNotes(firstNotNull_employeeInCharge_notes);
					// attribute 'Employees.photoPath'
					String firstNotNull_employeeInCharge_photoPath = Util.getStringValue(r.getAs("employeeInCharge_0.photoPath"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employeeInCharge_photoPath2 = Util.getStringValue(r.getAs("employeeInCharge_" + i + ".photoPath"));
						if (firstNotNull_employeeInCharge_photoPath != null && employeeInCharge_photoPath2 != null && !firstNotNull_employeeInCharge_photoPath.equals(employeeInCharge_photoPath2)) {
							register_res.addLogEvent("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.photoPath': " + firstNotNull_employeeInCharge_photoPath + " and " + employeeInCharge_photoPath2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.photoPath': " + firstNotNull_employeeInCharge_photoPath + " and " + employeeInCharge_photoPath2 + "." );
						}
						if (firstNotNull_employeeInCharge_photoPath == null && employeeInCharge_photoPath2 != null) {
							firstNotNull_employeeInCharge_photoPath = employeeInCharge_photoPath2;
						}
					}
					employeeInCharge_res.setPhotoPath(firstNotNull_employeeInCharge_photoPath);
					// attribute 'Employees.salary'
					Double firstNotNull_employeeInCharge_salary = Util.getDoubleValue(r.getAs("employeeInCharge_0.salary"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double employeeInCharge_salary2 = Util.getDoubleValue(r.getAs("employeeInCharge_" + i + ".salary"));
						if (firstNotNull_employeeInCharge_salary != null && employeeInCharge_salary2 != null && !firstNotNull_employeeInCharge_salary.equals(employeeInCharge_salary2)) {
							register_res.addLogEvent("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.salary': " + firstNotNull_employeeInCharge_salary + " and " + employeeInCharge_salary2 + "." );
							logger.warn("Data consistency problem for [Employees - id :"+employeeInCharge_res.getEmployeeID()+"]: different values found for attribute 'Employees.salary': " + firstNotNull_employeeInCharge_salary + " and " + employeeInCharge_salary2 + "." );
						}
						if (firstNotNull_employeeInCharge_salary == null && employeeInCharge_salary2 != null) {
							firstNotNull_employeeInCharge_salary = employeeInCharge_salary2;
						}
					}
					employeeInCharge_res.setSalary(firstNotNull_employeeInCharge_salary);
	
					register_res.setProcessedOrder(processedOrder_res);
					register_res.setEmployeeInCharge(employeeInCharge_res);
					return register_res;
		}
		, Encoders.bean(Register.class));
	
	}
	
	//Empty arguments
	public Dataset<Register> getRegisterList(){
		 return getRegisterList(null,null);
	}
	
	public abstract Dataset<Register> getRegisterList(
		Condition<OrdersAttribute> processedOrder_condition,
		Condition<EmployeesAttribute> employeeInCharge_condition);
	
	public Dataset<Register> getRegisterListByProcessedOrderCondition(
		Condition<OrdersAttribute> processedOrder_condition
	){
		return getRegisterList(processedOrder_condition, null);
	}
	
	public Register getRegisterByProcessedOrder(Orders processedOrder) {
		Condition<OrdersAttribute> cond = null;
		cond = Condition.simple(OrdersAttribute.id, Operator.EQUALS, processedOrder.getId());
		Dataset<Register> res = getRegisterListByProcessedOrderCondition(cond);
		List<Register> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	public Dataset<Register> getRegisterListByEmployeeInChargeCondition(
		Condition<EmployeesAttribute> employeeInCharge_condition
	){
		return getRegisterList(null, employeeInCharge_condition);
	}
	
	public Dataset<Register> getRegisterListByEmployeeInCharge(Employees employeeInCharge) {
		Condition<EmployeesAttribute> cond = null;
		cond = Condition.simple(EmployeesAttribute.employeeID, Operator.EQUALS, employeeInCharge.getEmployeeID());
		Dataset<Register> res = getRegisterListByEmployeeInChargeCondition(cond);
	return res;
	}
	
	
	
	public abstract void deleteRegisterList(
		conditions.Condition<conditions.OrdersAttribute> processedOrder_condition,
		conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition);
	
	public void deleteRegisterListByProcessedOrderCondition(
		conditions.Condition<conditions.OrdersAttribute> processedOrder_condition
	){
		deleteRegisterList(processedOrder_condition, null);
	}
	
	public void deleteRegisterByProcessedOrder(pojo.Orders processedOrder) {
		// TODO using id for selecting
		return;
	}
	public void deleteRegisterListByEmployeeInChargeCondition(
		conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition
	){
		deleteRegisterList(null, employeeInCharge_condition);
	}
	
	public void deleteRegisterListByEmployeeInCharge(pojo.Employees employeeInCharge) {
		// TODO using id for selecting
		return;
	}
		
}
