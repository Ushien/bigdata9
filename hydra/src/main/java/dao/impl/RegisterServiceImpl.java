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
import conditions.RegisterAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.OrdersTDO;
import tdo.RegisterTDO;
import conditions.OrdersAttribute;
import dao.services.OrdersService;
import tdo.EmployeesTDO;
import tdo.RegisterTDO;
import conditions.EmployeesAttribute;
import dao.services.EmployeesService;
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

public class RegisterServiceImpl extends dao.services.RegisterService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RegisterServiceImpl.class);
	
	
	// Left side 'EmployeeRef' of reference [encoded ]
	public Dataset<OrdersTDO> getOrdersTDOListProcessedOrderInEncodedInOrdersFromMongoDB(Condition<OrdersAttribute> condition, MutableBoolean refilterFlag){	
		String bsonQuery = OrdersServiceImpl.getBSONMatchQueryInOrdersFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Orders", bsonQuery);
	
		Dataset<OrdersTDO> res = dataset.flatMap((FlatMapFunction<Row, OrdersTDO>) r -> {
				Set<OrdersTDO> list_res = new HashSet<OrdersTDO>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				OrdersTDO orders1 = new OrdersTDO();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Orders.id for field OrderID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderID")) {
						if(nestedRow.getAs("OrderID") == null){
							orders1.setId(null);
						}else{
							orders1.setId(Util.getIntegerValue(nestedRow.getAs("OrderID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.OrderDate for field OrderDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderDate")) {
						if(nestedRow.getAs("OrderDate") == null){
							orders1.setOrderDate(null);
						}else{
							orders1.setOrderDate(Util.getLocalDateValue(nestedRow.getAs("OrderDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.RequiredDate for field RequiredDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("RequiredDate")) {
						if(nestedRow.getAs("RequiredDate") == null){
							orders1.setRequiredDate(null);
						}else{
							orders1.setRequiredDate(Util.getLocalDateValue(nestedRow.getAs("RequiredDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.Freight for field Freight			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Freight")) {
						if(nestedRow.getAs("Freight") == null){
							orders1.setFreight(null);
						}else{
							orders1.setFreight(Util.getDoubleValue(nestedRow.getAs("Freight")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.ShippedDate for field ShippedDate			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShippedDate")) {
						if(nestedRow.getAs("ShippedDate") == null){
							orders1.setShippedDate(null);
						}else{
							orders1.setShippedDate(Util.getLocalDateValue(nestedRow.getAs("ShippedDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.ShipName for field ShipName			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipName")) {
						if(nestedRow.getAs("ShipName") == null){
							orders1.setShipName(null);
						}else{
							orders1.setShipName(Util.getStringValue(nestedRow.getAs("ShipName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.ShipAddress for field ShipAddress			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipAddress")) {
						if(nestedRow.getAs("ShipAddress") == null){
							orders1.setShipAddress(null);
						}else{
							orders1.setShipAddress(Util.getStringValue(nestedRow.getAs("ShipAddress")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.ShipCity for field ShipCity			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCity")) {
						if(nestedRow.getAs("ShipCity") == null){
							orders1.setShipCity(null);
						}else{
							orders1.setShipCity(Util.getStringValue(nestedRow.getAs("ShipCity")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.ShipRegion for field ShipRegion			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipRegion")) {
						if(nestedRow.getAs("ShipRegion") == null){
							orders1.setShipRegion(null);
						}else{
							orders1.setShipRegion(Util.getStringValue(nestedRow.getAs("ShipRegion")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.ShipPostalCode for field ShipPostalCode			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipPostalCode")) {
						if(nestedRow.getAs("ShipPostalCode") == null){
							orders1.setShipPostalCode(null);
						}else{
							orders1.setShipPostalCode(Util.getStringValue(nestedRow.getAs("ShipPostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Orders.ShipCountry for field ShipCountry			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("ShipmentInfo");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCountry")) {
						if(nestedRow.getAs("ShipCountry") == null){
							orders1.setShipCountry(null);
						}else{
							orders1.setShipCountry(Util.getStringValue(nestedRow.getAs("ShipCountry")));
							toAdd1 = true;					
							}
					}
					
						// field  EmployeeRef for reference encoded . Reference field : EmployeeRef
					nestedRow =  r1;
					if(nestedRow != null) {
						orders1.setMongoDB_Orders_encoded_EmployeeRef(nestedRow.getAs("EmployeeRef") == null ? null : nestedRow.getAs("EmployeeRef").toString());
						toAdd1 = true;					
					}
					
					
					if(toAdd1) {
						list_res.add(orders1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(OrdersTDO.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
	}
	
	// Right side 'EmployeeID' of reference [encoded ]
	public Dataset<EmployeesTDO> getEmployeesTDOListEmployeeInChargeInEncodedInOrdersFromMongoDB(Condition<EmployeesAttribute> condition, MutableBoolean refilterFlag){
	
		Pair<String, List<String>> whereClause = EmployeesServiceImpl.getSQLWhereClauseInEmployeesFromMyRelDB(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", preparedValue);
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("myRelDB", "Employees", where);
		
	
		Dataset<EmployeesTDO> res = d.map((MapFunction<Row, EmployeesTDO>) r -> {
					EmployeesTDO employees_res = new EmployeesTDO();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
					// attribute [Employees.EmployeeID]
					Integer employeeID = Util.getIntegerValue(r.getAs("EmployeeID"));
					employees_res.setEmployeeID(employeeID);
					
					// attribute [Employees.LastName]
					String lastName = Util.getStringValue(r.getAs("LastName"));
					employees_res.setLastName(lastName);
					
					// attribute [Employees.FirstName]
					String firstName = Util.getStringValue(r.getAs("FirstName"));
					employees_res.setFirstName(firstName);
					
					// attribute [Employees.Title]
					String title = Util.getStringValue(r.getAs("Title"));
					employees_res.setTitle(title);
					
					// attribute [Employees.TitleOfCourtesy]
					String titleOfCourtesy = Util.getStringValue(r.getAs("TitleOfCourtesy"));
					employees_res.setTitleOfCourtesy(titleOfCourtesy);
					
					// attribute [Employees.BirthDate]
					LocalDate birthDate = Util.getLocalDateValue(r.getAs("BirthDate"));
					employees_res.setBirthDate(birthDate);
					
					// attribute [Employees.HireDate]
					LocalDate hireDate = Util.getLocalDateValue(r.getAs("HireDate"));
					employees_res.setHireDate(hireDate);
					
					// attribute [Employees.Address]
					String address = Util.getStringValue(r.getAs("Address"));
					employees_res.setAddress(address);
					
					// attribute [Employees.City]
					String city = Util.getStringValue(r.getAs("City"));
					employees_res.setCity(city);
					
					// attribute [Employees.Region]
					String region = Util.getStringValue(r.getAs("Region"));
					employees_res.setRegion(region);
					
					// attribute [Employees.PostalCode]
					String postalCode = Util.getStringValue(r.getAs("PostalCode"));
					employees_res.setPostalCode(postalCode);
					
					// attribute [Employees.Country]
					String country = Util.getStringValue(r.getAs("Country"));
					employees_res.setCountry(country);
					
					// attribute [Employees.HomePhone]
					String homePhone = Util.getStringValue(r.getAs("HomePhone"));
					employees_res.setHomePhone(homePhone);
					
					// attribute [Employees.Extension]
					String extension = Util.getStringValue(r.getAs("Extension"));
					employees_res.setExtension(extension);
					
					// attribute [Employees.Photo]
					byte[] photo = Util.getByteArrayValue(r.getAs("Photo"));
					employees_res.setPhoto(photo);
					
					// attribute [Employees.Notes]
					String notes = Util.getStringValue(r.getAs("Notes"));
					employees_res.setNotes(notes);
					
					// attribute [Employees.PhotoPath]
					String photoPath = Util.getStringValue(r.getAs("PhotoPath"));
					employees_res.setPhotoPath(photoPath);
					
					// attribute [Employees.Salary]
					Double salary = Util.getDoubleValue(r.getAs("Salary"));
					employees_res.setSalary(salary);
	
					// Get reference column [EmployeeID ] for reference [encoded]
					String mongoDB_Orders_encoded_EmployeeID = r.getAs("EmployeeID") == null ? null : r.getAs("EmployeeID").toString();
					employees_res.setMongoDB_Orders_encoded_EmployeeID(mongoDB_Orders_encoded_EmployeeID);
	
	
					return employees_res;
				}, Encoders.bean(EmployeesTDO.class));
	
	
		return res;}
	
	
	
	
	public Dataset<Register> getRegisterList(
		Condition<OrdersAttribute> processedOrder_condition,
		Condition<EmployeesAttribute> employeeInCharge_condition){
			RegisterServiceImpl registerService = this;
			OrdersService ordersService = new OrdersServiceImpl();  
			EmployeesService employeesService = new EmployeesServiceImpl();
			MutableBoolean processedOrder_refilter = new MutableBoolean(false);
			List<Dataset<Register>> datasetsPOJO = new ArrayList<Dataset<Register>>();
			boolean all_already_persisted = false;
			MutableBoolean employeeInCharge_refilter = new MutableBoolean(false);
			
			org.apache.spark.sql.Column joinCondition = null;
			// For role 'processedOrder' in reference 'encoded'. A->B Scenario
			employeeInCharge_refilter = new MutableBoolean(false);
			Dataset<OrdersTDO> ordersTDOencodedprocessedOrder = registerService.getOrdersTDOListProcessedOrderInEncodedInOrdersFromMongoDB(processedOrder_condition, processedOrder_refilter);
			Dataset<EmployeesTDO> employeesTDOencodedemployeeInCharge = registerService.getEmployeesTDOListEmployeeInChargeInEncodedInOrdersFromMongoDB(employeeInCharge_condition, employeeInCharge_refilter);
			
			Dataset<Row> res_encoded_temp = ordersTDOencodedprocessedOrder.join(employeesTDOencodedemployeeInCharge
					.withColumnRenamed("employeeID", "Employees_employeeID")
					.withColumnRenamed("lastName", "Employees_lastName")
					.withColumnRenamed("firstName", "Employees_firstName")
					.withColumnRenamed("title", "Employees_title")
					.withColumnRenamed("titleOfCourtesy", "Employees_titleOfCourtesy")
					.withColumnRenamed("birthDate", "Employees_birthDate")
					.withColumnRenamed("hireDate", "Employees_hireDate")
					.withColumnRenamed("address", "Employees_address")
					.withColumnRenamed("city", "Employees_city")
					.withColumnRenamed("region", "Employees_region")
					.withColumnRenamed("postalCode", "Employees_postalCode")
					.withColumnRenamed("country", "Employees_country")
					.withColumnRenamed("homePhone", "Employees_homePhone")
					.withColumnRenamed("extension", "Employees_extension")
					.withColumnRenamed("photo", "Employees_photo")
					.withColumnRenamed("notes", "Employees_notes")
					.withColumnRenamed("photoPath", "Employees_photoPath")
					.withColumnRenamed("salary", "Employees_salary")
					.withColumnRenamed("logEvents", "Employees_logEvents"),
					ordersTDOencodedprocessedOrder.col("mongoDB_Orders_encoded_EmployeeRef").equalTo(employeesTDOencodedemployeeInCharge.col("mongoDB_Orders_encoded_EmployeeID")));
		
			Dataset<Register> res_encoded = res_encoded_temp.map(
				(MapFunction<Row, Register>) r -> {
					Register res = new Register();
					Orders A = new Orders();
					Employees B = new Employees();
					A.setId(Util.getIntegerValue(r.getAs("id")));
					A.setOrderDate(Util.getLocalDateValue(r.getAs("orderDate")));
					A.setRequiredDate(Util.getLocalDateValue(r.getAs("requiredDate")));
					A.setShippedDate(Util.getLocalDateValue(r.getAs("shippedDate")));
					A.setFreight(Util.getDoubleValue(r.getAs("freight")));
					A.setShipName(Util.getStringValue(r.getAs("shipName")));
					A.setShipAddress(Util.getStringValue(r.getAs("shipAddress")));
					A.setShipCity(Util.getStringValue(r.getAs("shipCity")));
					A.setShipRegion(Util.getStringValue(r.getAs("shipRegion")));
					A.setShipPostalCode(Util.getStringValue(r.getAs("shipPostalCode")));
					A.setShipCountry(Util.getStringValue(r.getAs("shipCountry")));
					A.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("logEvents")));
		
					B.setEmployeeID(Util.getIntegerValue(r.getAs("Employees_employeeID")));
					B.setLastName(Util.getStringValue(r.getAs("Employees_lastName")));
					B.setFirstName(Util.getStringValue(r.getAs("Employees_firstName")));
					B.setTitle(Util.getStringValue(r.getAs("Employees_title")));
					B.setTitleOfCourtesy(Util.getStringValue(r.getAs("Employees_titleOfCourtesy")));
					B.setBirthDate(Util.getLocalDateValue(r.getAs("Employees_birthDate")));
					B.setHireDate(Util.getLocalDateValue(r.getAs("Employees_hireDate")));
					B.setAddress(Util.getStringValue(r.getAs("Employees_address")));
					B.setCity(Util.getStringValue(r.getAs("Employees_city")));
					B.setRegion(Util.getStringValue(r.getAs("Employees_region")));
					B.setPostalCode(Util.getStringValue(r.getAs("Employees_postalCode")));
					B.setCountry(Util.getStringValue(r.getAs("Employees_country")));
					B.setHomePhone(Util.getStringValue(r.getAs("Employees_homePhone")));
					B.setExtension(Util.getStringValue(r.getAs("Employees_extension")));
					B.setPhoto(Util.getByteArrayValue(r.getAs("Employees_photo")));
					B.setNotes(Util.getStringValue(r.getAs("Employees_notes")));
					B.setPhotoPath(Util.getStringValue(r.getAs("Employees_photoPath")));
					B.setSalary(Util.getDoubleValue(r.getAs("Employees_salary")));
					B.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("Employees_logEvents")));
						
					res.setProcessedOrder(A);
					res.setEmployeeInCharge(B);
					return res;
				},Encoders.bean(Register.class)
			);
		
			datasetsPOJO.add(res_encoded);
		
			
			Dataset<Register> res_register_processedOrder;
			Dataset<Orders> res_Orders;
			
			
			//Join datasets or return 
			Dataset<Register> res = fullOuterJoinsRegister(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Orders> lonelyProcessedOrder = null;
			Dataset<Employees> lonelyEmployeeInCharge = null;
			
		
		
			
			if(processedOrder_refilter.booleanValue() || employeeInCharge_refilter.booleanValue())
				res = res.filter((FilterFunction<Register>) r -> (processedOrder_condition == null || processedOrder_condition.evaluate(r.getProcessedOrder())) && (employeeInCharge_condition == null || employeeInCharge_condition.evaluate(r.getEmployeeInCharge())));
			
		
			return res;
		
		}
	
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
	
	
	
	public void deleteRegisterList(
		conditions.Condition<conditions.OrdersAttribute> processedOrder_condition,
		conditions.Condition<conditions.EmployeesAttribute> employeeInCharge_condition){
			//TODO
		}
	
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
