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
import conditions.WorksAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.EmployeesTDO;
import tdo.WorksTDO;
import conditions.EmployeesAttribute;
import dao.services.EmployeesService;
import tdo.TerritoriesTDO;
import tdo.WorksTDO;
import conditions.TerritoriesAttribute;
import dao.services.TerritoriesService;
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

public class WorksServiceImpl extends dao.services.WorksService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WorksServiceImpl.class);
	// A<-AB->B . getAListInREL
	public static Pair<String, List<String>> getSQLWhereClauseInEmployeeTerritoriesFromMyRelDB(Condition<WorksAttribute> condition, MutableBoolean refilterFlag) {
		return getSQLWhereClauseInEmployeeTerritoriesFromMyRelDBWithTableAlias(condition, refilterFlag, "");
	}
	
	public static List<String> getSQLSetClauseInEmployeeTerritoriesFromMyRelDB(conditions.SetClause<WorksAttribute> set) {
		List<String> res = new ArrayList<String>();
		if(set != null) {
			java.util.Map<String, java.util.Map<String, String>> longFieldValues = new java.util.HashMap<String, java.util.Map<String, String>>();
			java.util.Map<WorksAttribute, Object> clause = set.getClause();
			for(java.util.Map.Entry<WorksAttribute, Object> e : clause.entrySet()) {
				WorksAttribute attr = e.getKey();
				Object value = e.getValue();
			}
	
			for(java.util.Map.Entry<String, java.util.Map<String, String>> entry : longFieldValues.entrySet()) {
				String longField = entry.getKey();
				java.util.Map<String, String> values = entry.getValue();
			}
	
		}
		return res;
	}
	
	public static Pair<String, List<String>> getSQLWhereClauseInEmployeeTerritoriesFromMyRelDBWithTableAlias(Condition<WorksAttribute> condition, MutableBoolean refilterFlag, String tableAlias) {
		String where = null;	
		List<String> preparedValues = new java.util.ArrayList<String>();
		if(condition != null) {
			
			if(condition instanceof SimpleCondition) {
				WorksAttribute attr = ((SimpleCondition<WorksAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<WorksAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<WorksAttribute>) condition).getValue();
				if(value != null) {
					boolean isConditionAttrEncountered = false;
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						where = "1 = 1";
					}
				} else {
				}
			}
	
			if(condition instanceof AndCondition) {
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInEmployeeTerritoriesFromMyRelDB(((AndCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInEmployeeTerritoriesFromMyRelDB(((AndCondition) condition).getRightCondition(), refilterFlag);
				String whereLeft = pairLeft.getKey();
				String whereRight = pairRight.getKey();
				List<String> leftValues = pairLeft.getValue();
				List<String> rightValues = pairRight.getValue();
				if(whereLeft != null || whereRight != null) {
					if(whereLeft == null)
						where = whereRight;
					else
						if(whereRight == null)
							where = whereLeft;
						else
							where = "(" + whereLeft + " AND " + whereRight + ")";
					preparedValues.addAll(leftValues);
					preparedValues.addAll(rightValues);
				}
			}
	
			if(condition instanceof OrCondition) {
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInEmployeeTerritoriesFromMyRelDB(((OrCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInEmployeeTerritoriesFromMyRelDB(((OrCondition) condition).getRightCondition(), refilterFlag);
				String whereLeft = pairLeft.getKey();
				String whereRight = pairRight.getKey();
				List<String> leftValues = pairLeft.getValue();
				List<String> rightValues = pairRight.getValue();
				if(whereLeft != null || whereRight != null) {
					if(whereLeft == null)
						where = whereRight;
					else
						if(whereRight == null)
							where = whereLeft;
						else
							where = "(" + whereLeft + " OR " + whereRight + ")";
					preparedValues.addAll(leftValues);
					preparedValues.addAll(rightValues);
				}
			}
	
		}
	
		return new ImmutablePair<String, List<String>>(where, preparedValues);
	}
	
	public Dataset<Works> getWorksListInEmployeesAndEmployeeTerritoriesAndTerritoriesFrommyRelDB(Condition<EmployeesAttribute> employed_cond, Condition<TerritoriesAttribute> territories_cond, MutableBoolean employed_refilter, MutableBoolean territories_refilter){
		Pair<String, List<String>> whereClause1 = EmployeesServiceImpl.getSQLWhereClauseInEmployeesFromMyRelDBWithTableAlias(employed_cond, employed_refilter, "EmployeesA.");
		Pair<String, List<String>> whereClause2 = TerritoriesServiceImpl.getSQLWhereClauseInTerritoriesFromMyRelDBWithTableAlias(territories_cond, territories_refilter, "TerritoriesB.");
		
	
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
		String where = "";
		if(where1 != null)
			where += " AND " + where1;
		if(where2 != null)
			where += " AND " + where2;
		String aliasedColumns = "EmployeesA.EmployeeID as EmployeesA_EmployeeID,EmployeesA.LastName as EmployeesA_LastName,EmployeesA.FirstName as EmployeesA_FirstName,EmployeesA.Title as EmployeesA_Title,EmployeesA.TitleOfCourtesy as EmployeesA_TitleOfCourtesy,EmployeesA.BirthDate as EmployeesA_BirthDate,EmployeesA.HireDate as EmployeesA_HireDate,EmployeesA.HomePhone as EmployeesA_HomePhone,EmployeesA.Extension as EmployeesA_Extension,EmployeesA.Photo as EmployeesA_Photo,EmployeesA.Notes as EmployeesA_Notes,EmployeesA.PhotoPath as EmployeesA_PhotoPath,EmployeesA.Salary as EmployeesA_Salary,EmployeesA.Address as EmployeesA_Address,EmployeesA.City as EmployeesA_City,EmployeesA.Region as EmployeesA_Region,EmployeesA.PostalCode as EmployeesA_PostalCode,EmployeesA.Country as EmployeesA_Country,EmployeesA.ReportsTo as EmployeesA_ReportsTo,TerritoriesB.TerritoryID as TerritoriesB_TerritoryID,TerritoriesB.TerritoryDescription as TerritoriesB_TerritoryDescription,TerritoriesB.RegionRef as TerritoriesB_RegionRef,EmployeeTerritories.EmployeeRef as EmployeeTerritories_EmployeeRef,EmployeeTerritories.TerritoryRef as EmployeeTerritories_TerritoryRef";
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("myRelDB", "(SELECT " + aliasedColumns + " FROM Employees EmployeesA, Territories TerritoriesB, EmployeeTerritories WHERE EmployeeTerritories.EmployeeRef = EmployeesA.EmployeeID AND EmployeeTerritories.TerritoryRef = TerritoriesB.TerritoryID" + where + ") AS JOIN_TABLE", null);
		Dataset<Works> res = d.map((MapFunction<Row, Works>) r -> {
					Works works_res = new Works();
					works_res.setEmployed(new Employees());
					works_res.setTerritories(new Territories());
					
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
	
	
					
					// attribute [Employees.EmployeeID]
					Integer employed_employeeID = Util.getIntegerValue(r.getAs("EmployeesA_EmployeeID"));
					works_res.getEmployed().setEmployeeID(employed_employeeID);
					
					// attribute [Employees.LastName]
					String employed_lastName = Util.getStringValue(r.getAs("EmployeesA_LastName"));
					works_res.getEmployed().setLastName(employed_lastName);
					
					// attribute [Employees.FirstName]
					String employed_firstName = Util.getStringValue(r.getAs("EmployeesA_FirstName"));
					works_res.getEmployed().setFirstName(employed_firstName);
					
					// attribute [Employees.Title]
					String employed_title = Util.getStringValue(r.getAs("EmployeesA_Title"));
					works_res.getEmployed().setTitle(employed_title);
					
					// attribute [Employees.TitleOfCourtesy]
					String employed_titleOfCourtesy = Util.getStringValue(r.getAs("EmployeesA_TitleOfCourtesy"));
					works_res.getEmployed().setTitleOfCourtesy(employed_titleOfCourtesy);
					
					// attribute [Employees.BirthDate]
					LocalDate employed_birthDate = Util.getLocalDateValue(r.getAs("EmployeesA_BirthDate"));
					works_res.getEmployed().setBirthDate(employed_birthDate);
					
					// attribute [Employees.HireDate]
					LocalDate employed_hireDate = Util.getLocalDateValue(r.getAs("EmployeesA_HireDate"));
					works_res.getEmployed().setHireDate(employed_hireDate);
					
					// attribute [Employees.Address]
					String employed_address = Util.getStringValue(r.getAs("EmployeesA_Address"));
					works_res.getEmployed().setAddress(employed_address);
					
					// attribute [Employees.City]
					String employed_city = Util.getStringValue(r.getAs("EmployeesA_City"));
					works_res.getEmployed().setCity(employed_city);
					
					// attribute [Employees.Region]
					String employed_region = Util.getStringValue(r.getAs("EmployeesA_Region"));
					works_res.getEmployed().setRegion(employed_region);
					
					// attribute [Employees.PostalCode]
					String employed_postalCode = Util.getStringValue(r.getAs("EmployeesA_PostalCode"));
					works_res.getEmployed().setPostalCode(employed_postalCode);
					
					// attribute [Employees.Country]
					String employed_country = Util.getStringValue(r.getAs("EmployeesA_Country"));
					works_res.getEmployed().setCountry(employed_country);
					
					// attribute [Employees.HomePhone]
					String employed_homePhone = Util.getStringValue(r.getAs("EmployeesA_HomePhone"));
					works_res.getEmployed().setHomePhone(employed_homePhone);
					
					// attribute [Employees.Extension]
					String employed_extension = Util.getStringValue(r.getAs("EmployeesA_Extension"));
					works_res.getEmployed().setExtension(employed_extension);
					
					// attribute [Employees.Photo]
					byte[] employed_photo = Util.getByteArrayValue(r.getAs("EmployeesA_Photo"));
					works_res.getEmployed().setPhoto(employed_photo);
					
					// attribute [Employees.Notes]
					String employed_notes = Util.getStringValue(r.getAs("EmployeesA_Notes"));
					works_res.getEmployed().setNotes(employed_notes);
					
					// attribute [Employees.PhotoPath]
					String employed_photoPath = Util.getStringValue(r.getAs("EmployeesA_PhotoPath"));
					works_res.getEmployed().setPhotoPath(employed_photoPath);
					
					// attribute [Employees.Salary]
					Double employed_salary = Util.getDoubleValue(r.getAs("EmployeesA_Salary"));
					works_res.getEmployed().setSalary(employed_salary);
	
					
					// attribute [Territories.TerritoryID]
					String territories_territoryID = Util.getStringValue(r.getAs("TerritoriesB_TerritoryID"));
					works_res.getTerritories().setTerritoryID(territories_territoryID);
					
					// attribute [Territories.TerritoryDescription]
					String territories_territoryDescription = Util.getStringValue(r.getAs("TerritoriesB_TerritoryDescription"));
					works_res.getTerritories().setTerritoryDescription(territories_territoryDescription);
	
					return works_res;
				}, Encoders.bean(Works.class));
	
		return res;
	}
	
	
	// A<-AB->B . getBListInREL
	
	
	public Dataset<Works> getWorksListInTerritoriesAndEmployeeTerritoriesAndEmployeesFrommyRelDB(Condition<TerritoriesAttribute> territories_cond, Condition<EmployeesAttribute> employed_cond, MutableBoolean territories_refilter, MutableBoolean employed_refilter){
		Pair<String, List<String>> whereClause1 = TerritoriesServiceImpl.getSQLWhereClauseInTerritoriesFromMyRelDBWithTableAlias(territories_cond, territories_refilter, "TerritoriesA.");
		Pair<String, List<String>> whereClause2 = EmployeesServiceImpl.getSQLWhereClauseInEmployeesFromMyRelDBWithTableAlias(employed_cond, employed_refilter, "EmployeesB.");
		
	
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
		String where = "";
		if(where1 != null)
			where += " AND " + where1;
		if(where2 != null)
			where += " AND " + where2;
		String aliasedColumns = "TerritoriesA.TerritoryID as TerritoriesA_TerritoryID,TerritoriesA.TerritoryDescription as TerritoriesA_TerritoryDescription,TerritoriesA.RegionRef as TerritoriesA_RegionRef,EmployeesB.EmployeeID as EmployeesB_EmployeeID,EmployeesB.LastName as EmployeesB_LastName,EmployeesB.FirstName as EmployeesB_FirstName,EmployeesB.Title as EmployeesB_Title,EmployeesB.TitleOfCourtesy as EmployeesB_TitleOfCourtesy,EmployeesB.BirthDate as EmployeesB_BirthDate,EmployeesB.HireDate as EmployeesB_HireDate,EmployeesB.HomePhone as EmployeesB_HomePhone,EmployeesB.Extension as EmployeesB_Extension,EmployeesB.Photo as EmployeesB_Photo,EmployeesB.Notes as EmployeesB_Notes,EmployeesB.PhotoPath as EmployeesB_PhotoPath,EmployeesB.Salary as EmployeesB_Salary,EmployeesB.Address as EmployeesB_Address,EmployeesB.City as EmployeesB_City,EmployeesB.Region as EmployeesB_Region,EmployeesB.PostalCode as EmployeesB_PostalCode,EmployeesB.Country as EmployeesB_Country,EmployeesB.ReportsTo as EmployeesB_ReportsTo,EmployeeTerritories.EmployeeRef as EmployeeTerritories_EmployeeRef,EmployeeTerritories.TerritoryRef as EmployeeTerritories_TerritoryRef";
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("myRelDB", "(SELECT " + aliasedColumns + " FROM Territories TerritoriesA, Employees EmployeesB, EmployeeTerritories WHERE EmployeeTerritories.TerritoryRef = TerritoriesA.TerritoryID AND EmployeeTerritories.EmployeeRef = EmployeesB.EmployeeID" + where + ") AS JOIN_TABLE", null);
		Dataset<Works> res = d.map((MapFunction<Row, Works>) r -> {
					Works works_res = new Works();
					works_res.setTerritories(new Territories());
					works_res.setEmployed(new Employees());
					
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
	
	
					
					// attribute [Territories.TerritoryID]
					String territories_territoryID = Util.getStringValue(r.getAs("TerritoriesA_TerritoryID"));
					works_res.getTerritories().setTerritoryID(territories_territoryID);
					
					// attribute [Territories.TerritoryDescription]
					String territories_territoryDescription = Util.getStringValue(r.getAs("TerritoriesA_TerritoryDescription"));
					works_res.getTerritories().setTerritoryDescription(territories_territoryDescription);
	
					
					// attribute [Employed.EmployeeID]
					Integer employed_employeeID = Util.getIntegerValue(r.getAs("EmployeesB_EmployeeID"));
					works_res.getEmployed().setEmployeeID(employed_employeeID);
					
					// attribute [Employed.LastName]
					String employed_lastName = Util.getStringValue(r.getAs("EmployeesB_LastName"));
					works_res.getEmployed().setLastName(employed_lastName);
					
					// attribute [Employed.FirstName]
					String employed_firstName = Util.getStringValue(r.getAs("EmployeesB_FirstName"));
					works_res.getEmployed().setFirstName(employed_firstName);
					
					// attribute [Employed.Title]
					String employed_title = Util.getStringValue(r.getAs("EmployeesB_Title"));
					works_res.getEmployed().setTitle(employed_title);
					
					// attribute [Employed.TitleOfCourtesy]
					String employed_titleOfCourtesy = Util.getStringValue(r.getAs("EmployeesB_TitleOfCourtesy"));
					works_res.getEmployed().setTitleOfCourtesy(employed_titleOfCourtesy);
					
					// attribute [Employed.BirthDate]
					LocalDate employed_birthDate = Util.getLocalDateValue(r.getAs("EmployeesB_BirthDate"));
					works_res.getEmployed().setBirthDate(employed_birthDate);
					
					// attribute [Employed.HireDate]
					LocalDate employed_hireDate = Util.getLocalDateValue(r.getAs("EmployeesB_HireDate"));
					works_res.getEmployed().setHireDate(employed_hireDate);
					
					// attribute [Employed.Address]
					String employed_address = Util.getStringValue(r.getAs("EmployeesB_Address"));
					works_res.getEmployed().setAddress(employed_address);
					
					// attribute [Employed.City]
					String employed_city = Util.getStringValue(r.getAs("EmployeesB_City"));
					works_res.getEmployed().setCity(employed_city);
					
					// attribute [Employed.Region]
					String employed_region = Util.getStringValue(r.getAs("EmployeesB_Region"));
					works_res.getEmployed().setRegion(employed_region);
					
					// attribute [Employed.PostalCode]
					String employed_postalCode = Util.getStringValue(r.getAs("EmployeesB_PostalCode"));
					works_res.getEmployed().setPostalCode(employed_postalCode);
					
					// attribute [Employed.Country]
					String employed_country = Util.getStringValue(r.getAs("EmployeesB_Country"));
					works_res.getEmployed().setCountry(employed_country);
					
					// attribute [Employed.HomePhone]
					String employed_homePhone = Util.getStringValue(r.getAs("EmployeesB_HomePhone"));
					works_res.getEmployed().setHomePhone(employed_homePhone);
					
					// attribute [Employed.Extension]
					String employed_extension = Util.getStringValue(r.getAs("EmployeesB_Extension"));
					works_res.getEmployed().setExtension(employed_extension);
					
					// attribute [Employed.Photo]
					byte[] employed_photo = Util.getByteArrayValue(r.getAs("EmployeesB_Photo"));
					works_res.getEmployed().setPhoto(employed_photo);
					
					// attribute [Employed.Notes]
					String employed_notes = Util.getStringValue(r.getAs("EmployeesB_Notes"));
					works_res.getEmployed().setNotes(employed_notes);
					
					// attribute [Employed.PhotoPath]
					String employed_photoPath = Util.getStringValue(r.getAs("EmployeesB_PhotoPath"));
					works_res.getEmployed().setPhotoPath(employed_photoPath);
					
					// attribute [Employed.Salary]
					Double employed_salary = Util.getDoubleValue(r.getAs("EmployeesB_Salary"));
					works_res.getEmployed().setSalary(employed_salary);
	
					return works_res;
				}, Encoders.bean(Works.class));
	
		return res;
	}
	
	
	
	
	
	public Dataset<Works> getWorksList(
		Condition<EmployeesAttribute> employed_condition,
		Condition<TerritoriesAttribute> territories_condition){
			WorksServiceImpl worksService = this;
			EmployeesService employeesService = new EmployeesServiceImpl();  
			TerritoriesService territoriesService = new TerritoriesServiceImpl();
			MutableBoolean employed_refilter = new MutableBoolean(false);
			List<Dataset<Works>> datasetsPOJO = new ArrayList<Dataset<Works>>();
			boolean all_already_persisted = false;
			MutableBoolean territories_refilter = new MutableBoolean(false);
			
			org.apache.spark.sql.Column joinCondition = null;
			// join physical structure A<-AB->B
			//join between 3 SQL tables stored in same db
			// (A AB B)
			territories_refilter = new MutableBoolean(false);
			Dataset<Works> res_Employees_employee_territory = worksService.getWorksListInEmployeesAndEmployeeTerritoriesAndTerritoriesFrommyRelDB(employed_condition, territories_condition, employed_refilter, territories_refilter);
			datasetsPOJO.add(res_Employees_employee_territory);	
			
		
			
			Dataset<Works> res_works_employed;
			Dataset<Employees> res_Employees;
			
			
			//Join datasets or return 
			Dataset<Works> res = fullOuterJoinsWorks(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Employees> lonelyEmployed = null;
			Dataset<Territories> lonelyTerritories = null;
			
		
		
			
			if(employed_refilter.booleanValue() || territories_refilter.booleanValue())
				res = res.filter((FilterFunction<Works>) r -> (employed_condition == null || employed_condition.evaluate(r.getEmployed())) && (territories_condition == null || territories_condition.evaluate(r.getTerritories())));
			
		
			return res;
		
		}
	
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
	
	public void insertWorks(Works works){
		//Link entities in join structures.
		insertWorksInJoinStructEmployeeTerritoriesInMyRelDB(works);
		// Update embedded structures mapped to non mandatory roles.
		// Update ref fields mapped to non mandatory roles. 
	}
	
	public 	boolean insertWorksInJoinStructEmployeeTerritoriesInMyRelDB(Works works){
	 	// Rel 'works' Insert in join structure 'EmployeeTerritories'
		
		Employees employed_employees = works.getEmployed();
		Territories territories_territories = works.getTerritories();
		List<String> columns = new ArrayList<>();
		List<Object> values = new ArrayList<>();
		List<List<Object>> rows = new ArrayList<>();
		// Role in join structure 
		columns.add("EmployeeRef");
		Object employeesId = employed_employees.getEmployeeID();
		values.add(employeesId);
		// Role in join structure 
		columns.add("TerritoryRef");
		Object territoriesId = territories_territories.getTerritoryID();
		values.add(territoriesId);
		rows.add(values);
		DBConnectionMgr.insertInTable(columns, rows, "EmployeeTerritories", "myRelDB"); 					
		return true;
	
	}
	
	
	
	
	
	
	public void deleteWorksList(
		conditions.Condition<conditions.EmployeesAttribute> employed_condition,
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition){
			//TODO
		}
	
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
