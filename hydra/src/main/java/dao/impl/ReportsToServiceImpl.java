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
import conditions.ReportsToAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.EmployeesTDO;
import tdo.ReportsToTDO;
import conditions.EmployeesAttribute;
import dao.services.EmployeesService;
import tdo.EmployeesTDO;
import tdo.ReportsToTDO;
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

public class ReportsToServiceImpl extends dao.services.ReportsToService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReportsToServiceImpl.class);
	
	public  Dataset<ReportsTo> getSubordoneeAndBossListInManagerInEmployeesFromMyRelDB(Condition<EmployeesAttribute> subordonee_cond, Condition<EmployeesAttribute> boss_cond, MutableBoolean subordonee_refilter, MutableBoolean boss_refilter) {
		Pair<String, List<String>> whereClause1 = EmployeesServiceImpl.getSQLWhereClauseInEmployeesFromMyRelDBWithTableAlias(subordonee_cond, subordonee_refilter, "EmployeesA.");
		Pair<String, List<String>> whereClause2 = EmployeesServiceImpl.getSQLWhereClauseInEmployeesFromMyRelDBWithTableAlias(boss_cond, boss_refilter, "EmployeesB.");
		
		String where1 = whereClause1.getKey();
		List<String> preparedValues = whereClause1.getValue();
		for(String preparedValue : preparedValues) {
			where1 = where1.replaceFirst("\\?", preparedValue);
		}
	
		String where2 = whereClause2.getKey();
		preparedValues = whereClause2.getValue();
		for(String preparedValue : preparedValues) {
			where2 = where2.replaceFirst("\\?", preparedValue);
		}
	
		String where = (where1 == null && where2 == null) ? "" : " AND " + (where1 != null && where2 != null ? where1 + " AND " + where2 : (where1 != null ? where1 : where2) );
		String aliasedColumns = "EmployeesA.EmployeeID as EmployeesA_EmployeeID,EmployeesA.LastName as EmployeesA_LastName,EmployeesA.FirstName as EmployeesA_FirstName,EmployeesA.Title as EmployeesA_Title,EmployeesA.TitleOfCourtesy as EmployeesA_TitleOfCourtesy,EmployeesA.BirthDate as EmployeesA_BirthDate,EmployeesA.HireDate as EmployeesA_HireDate,EmployeesA.HomePhone as EmployeesA_HomePhone,EmployeesA.Extension as EmployeesA_Extension,EmployeesA.Photo as EmployeesA_Photo,EmployeesA.Notes as EmployeesA_Notes,EmployeesA.PhotoPath as EmployeesA_PhotoPath,EmployeesA.Salary as EmployeesA_Salary,EmployeesA.Address as EmployeesA_Address,EmployeesA.City as EmployeesA_City,EmployeesA.Region as EmployeesA_Region,EmployeesA.PostalCode as EmployeesA_PostalCode,EmployeesA.Country as EmployeesA_Country,EmployeesA.ReportsTo as EmployeesA_ReportsTo,EmployeesB.EmployeeID as EmployeesB_EmployeeID,EmployeesB.LastName as EmployeesB_LastName,EmployeesB.FirstName as EmployeesB_FirstName,EmployeesB.Title as EmployeesB_Title,EmployeesB.TitleOfCourtesy as EmployeesB_TitleOfCourtesy,EmployeesB.BirthDate as EmployeesB_BirthDate,EmployeesB.HireDate as EmployeesB_HireDate,EmployeesB.HomePhone as EmployeesB_HomePhone,EmployeesB.Extension as EmployeesB_Extension,EmployeesB.Photo as EmployeesB_Photo,EmployeesB.Notes as EmployeesB_Notes,EmployeesB.PhotoPath as EmployeesB_PhotoPath,EmployeesB.Salary as EmployeesB_Salary,EmployeesB.Address as EmployeesB_Address,EmployeesB.City as EmployeesB_City,EmployeesB.Region as EmployeesB_Region,EmployeesB.PostalCode as EmployeesB_PostalCode,EmployeesB.Country as EmployeesB_Country,EmployeesB.ReportsTo as EmployeesB_ReportsTo";
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("myRelDB", "(SELECT " + aliasedColumns + " FROM Employees EmployeesA, Employees EmployeesB WHERE EmployeesA.ReportsTo = EmployeesB.EmployeeID" + where + ") AS JOIN_TABLE", null);
		
		Dataset<ReportsTo> res = d.map((MapFunction<Row, ReportsTo>) r -> {
					ReportsTo reportsTo_res = new ReportsTo();
					reportsTo_res.setSubordonee(new Employees());
					reportsTo_res.setBoss(new Employees());
					
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
	
	
					
					// attribute [Employees.EmployeeID]
					Integer subordonee_employeeID = Util.getIntegerValue(r.getAs("EmployeesA_EmployeeID"));
					reportsTo_res.getSubordonee().setEmployeeID(subordonee_employeeID);
					
					// attribute [Employees.LastName]
					String subordonee_lastName = Util.getStringValue(r.getAs("EmployeesA_LastName"));
					reportsTo_res.getSubordonee().setLastName(subordonee_lastName);
					
					// attribute [Employees.FirstName]
					String subordonee_firstName = Util.getStringValue(r.getAs("EmployeesA_FirstName"));
					reportsTo_res.getSubordonee().setFirstName(subordonee_firstName);
					
					// attribute [Employees.Title]
					String subordonee_title = Util.getStringValue(r.getAs("EmployeesA_Title"));
					reportsTo_res.getSubordonee().setTitle(subordonee_title);
					
					// attribute [Employees.TitleOfCourtesy]
					String subordonee_titleOfCourtesy = Util.getStringValue(r.getAs("EmployeesA_TitleOfCourtesy"));
					reportsTo_res.getSubordonee().setTitleOfCourtesy(subordonee_titleOfCourtesy);
					
					// attribute [Employees.BirthDate]
					LocalDate subordonee_birthDate = Util.getLocalDateValue(r.getAs("EmployeesA_BirthDate"));
					reportsTo_res.getSubordonee().setBirthDate(subordonee_birthDate);
					
					// attribute [Employees.HireDate]
					LocalDate subordonee_hireDate = Util.getLocalDateValue(r.getAs("EmployeesA_HireDate"));
					reportsTo_res.getSubordonee().setHireDate(subordonee_hireDate);
					
					// attribute [Employees.Address]
					String subordonee_address = Util.getStringValue(r.getAs("EmployeesA_Address"));
					reportsTo_res.getSubordonee().setAddress(subordonee_address);
					
					// attribute [Employees.City]
					String subordonee_city = Util.getStringValue(r.getAs("EmployeesA_City"));
					reportsTo_res.getSubordonee().setCity(subordonee_city);
					
					// attribute [Employees.Region]
					String subordonee_region = Util.getStringValue(r.getAs("EmployeesA_Region"));
					reportsTo_res.getSubordonee().setRegion(subordonee_region);
					
					// attribute [Employees.PostalCode]
					String subordonee_postalCode = Util.getStringValue(r.getAs("EmployeesA_PostalCode"));
					reportsTo_res.getSubordonee().setPostalCode(subordonee_postalCode);
					
					// attribute [Employees.Country]
					String subordonee_country = Util.getStringValue(r.getAs("EmployeesA_Country"));
					reportsTo_res.getSubordonee().setCountry(subordonee_country);
					
					// attribute [Employees.HomePhone]
					String subordonee_homePhone = Util.getStringValue(r.getAs("EmployeesA_HomePhone"));
					reportsTo_res.getSubordonee().setHomePhone(subordonee_homePhone);
					
					// attribute [Employees.Extension]
					String subordonee_extension = Util.getStringValue(r.getAs("EmployeesA_Extension"));
					reportsTo_res.getSubordonee().setExtension(subordonee_extension);
					
					// attribute [Employees.Photo]
					byte[] subordonee_photo = Util.getByteArrayValue(r.getAs("EmployeesA_Photo"));
					reportsTo_res.getSubordonee().setPhoto(subordonee_photo);
					
					// attribute [Employees.Notes]
					String subordonee_notes = Util.getStringValue(r.getAs("EmployeesA_Notes"));
					reportsTo_res.getSubordonee().setNotes(subordonee_notes);
					
					// attribute [Employees.PhotoPath]
					String subordonee_photoPath = Util.getStringValue(r.getAs("EmployeesA_PhotoPath"));
					reportsTo_res.getSubordonee().setPhotoPath(subordonee_photoPath);
					
					// attribute [Employees.Salary]
					Double subordonee_salary = Util.getDoubleValue(r.getAs("EmployeesA_Salary"));
					reportsTo_res.getSubordonee().setSalary(subordonee_salary);
	
					
					// attribute [Boss.EmployeeID]
					Integer boss_employeeID = Util.getIntegerValue(r.getAs("EmployeesB_EmployeeID"));
					reportsTo_res.getBoss().setEmployeeID(boss_employeeID);
					
					// attribute [Boss.LastName]
					String boss_lastName = Util.getStringValue(r.getAs("EmployeesB_LastName"));
					reportsTo_res.getBoss().setLastName(boss_lastName);
					
					// attribute [Boss.FirstName]
					String boss_firstName = Util.getStringValue(r.getAs("EmployeesB_FirstName"));
					reportsTo_res.getBoss().setFirstName(boss_firstName);
					
					// attribute [Boss.Title]
					String boss_title = Util.getStringValue(r.getAs("EmployeesB_Title"));
					reportsTo_res.getBoss().setTitle(boss_title);
					
					// attribute [Boss.TitleOfCourtesy]
					String boss_titleOfCourtesy = Util.getStringValue(r.getAs("EmployeesB_TitleOfCourtesy"));
					reportsTo_res.getBoss().setTitleOfCourtesy(boss_titleOfCourtesy);
					
					// attribute [Boss.BirthDate]
					LocalDate boss_birthDate = Util.getLocalDateValue(r.getAs("EmployeesB_BirthDate"));
					reportsTo_res.getBoss().setBirthDate(boss_birthDate);
					
					// attribute [Boss.HireDate]
					LocalDate boss_hireDate = Util.getLocalDateValue(r.getAs("EmployeesB_HireDate"));
					reportsTo_res.getBoss().setHireDate(boss_hireDate);
					
					// attribute [Boss.Address]
					String boss_address = Util.getStringValue(r.getAs("EmployeesB_Address"));
					reportsTo_res.getBoss().setAddress(boss_address);
					
					// attribute [Boss.City]
					String boss_city = Util.getStringValue(r.getAs("EmployeesB_City"));
					reportsTo_res.getBoss().setCity(boss_city);
					
					// attribute [Boss.Region]
					String boss_region = Util.getStringValue(r.getAs("EmployeesB_Region"));
					reportsTo_res.getBoss().setRegion(boss_region);
					
					// attribute [Boss.PostalCode]
					String boss_postalCode = Util.getStringValue(r.getAs("EmployeesB_PostalCode"));
					reportsTo_res.getBoss().setPostalCode(boss_postalCode);
					
					// attribute [Boss.Country]
					String boss_country = Util.getStringValue(r.getAs("EmployeesB_Country"));
					reportsTo_res.getBoss().setCountry(boss_country);
					
					// attribute [Boss.HomePhone]
					String boss_homePhone = Util.getStringValue(r.getAs("EmployeesB_HomePhone"));
					reportsTo_res.getBoss().setHomePhone(boss_homePhone);
					
					// attribute [Boss.Extension]
					String boss_extension = Util.getStringValue(r.getAs("EmployeesB_Extension"));
					reportsTo_res.getBoss().setExtension(boss_extension);
					
					// attribute [Boss.Photo]
					byte[] boss_photo = Util.getByteArrayValue(r.getAs("EmployeesB_Photo"));
					reportsTo_res.getBoss().setPhoto(boss_photo);
					
					// attribute [Boss.Notes]
					String boss_notes = Util.getStringValue(r.getAs("EmployeesB_Notes"));
					reportsTo_res.getBoss().setNotes(boss_notes);
					
					// attribute [Boss.PhotoPath]
					String boss_photoPath = Util.getStringValue(r.getAs("EmployeesB_PhotoPath"));
					reportsTo_res.getBoss().setPhotoPath(boss_photoPath);
					
					// attribute [Boss.Salary]
					Double boss_salary = Util.getDoubleValue(r.getAs("EmployeesB_Salary"));
					reportsTo_res.getBoss().setSalary(boss_salary);
	
					return reportsTo_res;
				}, Encoders.bean(ReportsTo.class));
	
		return res;
	}
	
	
	
	
	
	public Dataset<ReportsTo> getReportsToList(
		Condition<EmployeesAttribute> subordonee_condition,
		Condition<EmployeesAttribute> boss_condition){
			ReportsToServiceImpl reportsToService = this;
			EmployeesService employeesService = new EmployeesServiceImpl();  
			MutableBoolean subordonee_refilter = new MutableBoolean(false);
			List<Dataset<ReportsTo>> datasetsPOJO = new ArrayList<Dataset<ReportsTo>>();
			boolean all_already_persisted = false;
			MutableBoolean boss_refilter = new MutableBoolean(false);
			
			org.apache.spark.sql.Column joinCondition = null;
			// For role 'subordonee' in reference 'manager'. A->B Scenario in SQL DB
			boss_refilter = new MutableBoolean(false);
			Dataset<ReportsTo> res_manager = reportsToService.getSubordoneeAndBossListInManagerInEmployeesFromMyRelDB(subordonee_condition, boss_condition, subordonee_refilter, boss_refilter);
			datasetsPOJO.add(res_manager);
		
			
			Dataset<ReportsTo> res_reportsTo_subordonee;
			Dataset<Employees> res_Employees;
			
			
			//Join datasets or return 
			Dataset<ReportsTo> res = fullOuterJoinsReportsTo(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Employees> lonelySubordonee = null;
			Dataset<Employees> lonelyBoss = null;
			
		
		
			
			if(subordonee_refilter.booleanValue() || boss_refilter.booleanValue())
				res = res.filter((FilterFunction<ReportsTo>) r -> (subordonee_condition == null || subordonee_condition.evaluate(r.getSubordonee())) && (boss_condition == null || boss_condition.evaluate(r.getBoss())));
			
		
			return res;
		
		}
	
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
	
	public void insertReportsTo(ReportsTo reportsTo){
		//Link entities in join structures.
		// Update embedded structures mapped to non mandatory roles.
		// Update ref fields mapped to non mandatory roles. 
		insertReportsToInRefStructEmployeesInMyRelDB(reportsTo);
	}
	
	
	
	public 	boolean insertReportsToInRefStructEmployeesInMyRelDB(ReportsTo reportsTo){
	 	// Rel 'reportsTo' Insert in reference structure 'Employees'
		Employees employeesSubordonee = reportsTo.getSubordonee();
		Employees employeesBoss = reportsTo.getBoss();
	
		List<String> columns = new ArrayList<>();
		List<Object> values = new ArrayList<>();
		String filtercolumn;
		Object filtervalue;
		columns.add("ReportsTo");
		values.add(employeesBoss==null?null:employeesBoss.getEmployeeID());
		filtercolumn = "EmployeeID";
		filtervalue = employeesSubordonee.getEmployeeID();
		DBConnectionMgr.updateInTable(filtercolumn, filtervalue, columns, values, "Employees", "myRelDB");					
		return true;
	}
	
	
	
	
	public void deleteReportsToList(
		conditions.Condition<conditions.EmployeesAttribute> subordonee_condition,
		conditions.Condition<conditions.EmployeesAttribute> boss_condition){
			//TODO
		}
	
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
