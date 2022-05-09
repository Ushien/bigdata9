package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Territories;
import conditions.*;
import dao.services.TerritoriesService;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import util.Dataset;
import org.apache.spark.sql.Encoders;
import util.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.function.MapFunction;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import com.mongodb.spark.MongoSpark;
import org.bson.Document;
import static java.util.Collections.singletonList;
import dbconnection.SparkConnectionMgr;
import dbconnection.DBConnectionMgr;
import util.WrappedArray;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FilterFunction;
import java.util.ArrayList;
import org.apache.commons.lang.mutable.MutableBoolean;
import tdo.*;
import pojo.*;
import util.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.ArrayType;
import scala.Tuple2;
import org.bson.Document;
import org.bson.conversions.Bson;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;


public class TerritoriesServiceImpl extends TerritoriesService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TerritoriesServiceImpl.class);
	
	
	
	
	public static Pair<String, List<String>> getSQLWhereClauseInTerritoriesFromMyRelDB(Condition<TerritoriesAttribute> condition, MutableBoolean refilterFlag) {
		return getSQLWhereClauseInTerritoriesFromMyRelDBWithTableAlias(condition, refilterFlag, "");
	}
	
	public static List<String> getSQLSetClauseInTerritoriesFromMyRelDB(conditions.SetClause<TerritoriesAttribute> set) {
		List<String> res = new ArrayList<String>();
		if(set != null) {
			java.util.Map<String, java.util.Map<String, String>> longFieldValues = new java.util.HashMap<String, java.util.Map<String, String>>();
			java.util.Map<TerritoriesAttribute, Object> clause = set.getClause();
			for(java.util.Map.Entry<TerritoriesAttribute, Object> e : clause.entrySet()) {
				TerritoriesAttribute attr = e.getKey();
				Object value = e.getValue();
				if(attr == TerritoriesAttribute.territoryID ) {
					res.add("TerritoryID = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == TerritoriesAttribute.territoryDescription ) {
					res.add("TerritoryDescription = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
			}
	
			for(java.util.Map.Entry<String, java.util.Map<String, String>> entry : longFieldValues.entrySet()) {
				String longField = entry.getKey();
				java.util.Map<String, String> values = entry.getValue();
			}
	
		}
		return res;
	}
	
	public static Pair<String, List<String>> getSQLWhereClauseInTerritoriesFromMyRelDBWithTableAlias(Condition<TerritoriesAttribute> condition, MutableBoolean refilterFlag, String tableAlias) {
		String where = null;	
		List<String> preparedValues = new java.util.ArrayList<String>();
		if(condition != null) {
			
			if(condition instanceof SimpleCondition) {
				TerritoriesAttribute attr = ((SimpleCondition<TerritoriesAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<TerritoriesAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<TerritoriesAttribute>) condition).getValue();
				if(value != null) {
					boolean isConditionAttrEncountered = false;
					if(attr == TerritoriesAttribute.territoryID ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "TerritoryID " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == TerritoriesAttribute.territoryDescription ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "TerritoryDescription " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						where = "1 = 1";
					}
				} else {
					if(attr == TerritoriesAttribute.territoryID ) {
						if(op == Operator.EQUALS)
							where =  "TerritoryID IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "TerritoryID IS NOT NULL";
					}
					if(attr == TerritoriesAttribute.territoryDescription ) {
						if(op == Operator.EQUALS)
							where =  "TerritoryDescription IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "TerritoryDescription IS NOT NULL";
					}
				}
			}
	
			if(condition instanceof AndCondition) {
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInTerritoriesFromMyRelDB(((AndCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInTerritoriesFromMyRelDB(((AndCondition) condition).getRightCondition(), refilterFlag);
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
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInTerritoriesFromMyRelDB(((OrCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInTerritoriesFromMyRelDB(((OrCondition) condition).getRightCondition(), refilterFlag);
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
	
	
	
	public Dataset<Territories> getTerritoriesListInTerritoriesFromMyRelDB(conditions.Condition<conditions.TerritoriesAttribute> condition, MutableBoolean refilterFlag){
	
		Pair<String, List<String>> whereClause = TerritoriesServiceImpl.getSQLWhereClauseInTerritoriesFromMyRelDB(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", preparedValue);
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("myRelDB", "Territories", where);
		
	
		Dataset<Territories> res = d.map((MapFunction<Row, Territories>) r -> {
					Territories territories_res = new Territories();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
					// attribute [Territories.TerritoryID]
					String territoryID = Util.getStringValue(r.getAs("TerritoryID"));
					territories_res.setTerritoryID(territoryID);
					
					// attribute [Territories.TerritoryDescription]
					String territoryDescription = Util.getStringValue(r.getAs("TerritoryDescription"));
					territories_res.setTerritoryDescription(territoryDescription);
	
	
	
					return territories_res;
				}, Encoders.bean(Territories.class));
	
	
		return res;
		
	}
	
	
	
	
	
	
	public Dataset<Territories> getTerritoriesListInLocatedIn(conditions.Condition<conditions.TerritoriesAttribute> territories_condition,conditions.Condition<conditions.RegionAttribute> region_condition)		{
		MutableBoolean territories_refilter = new MutableBoolean(false);
		List<Dataset<Territories>> datasetsPOJO = new ArrayList<Dataset<Territories>>();
		Dataset<Region> all = null;
		boolean all_already_persisted = false;
		MutableBoolean region_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		// For role 'territories' in reference 'located'. A->B Scenario in SQL DB
		region_refilter = new MutableBoolean(false);
		Dataset<LocatedIn> res_located = locatedInService.getTerritoriesAndRegionListInLocatedInTerritoriesFromMyRelDB(territories_condition, region_condition, territories_refilter, region_refilter);
		Dataset<Territories> res_Territories_located = null;
		if(region_refilter.booleanValue()) {
			if(all == null)
				all = new RegionServiceImpl().getRegionList(region_condition);
			joinCondition = null;
			joinCondition = res_located.col("region.regionID").equalTo(all.col("regionID"));
			if(joinCondition == null)
				res_Territories_located = res_located.join(all).select("territories.*").as(Encoders.bean(Territories.class));
			else
				res_Territories_located = res_located.join(all, joinCondition).select("territories.*").as(Encoders.bean(Territories.class));
		} else
			res_Territories_located = res_located.map((MapFunction<LocatedIn,Territories>) r -> r.getTerritories(), Encoders.bean(Territories.class));
		res_Territories_located = res_Territories_located.dropDuplicates(new String[] {"territoryID"});
		datasetsPOJO.add(res_Territories_located);
		
		
		Dataset<LocatedIn> res_locatedIn_territories;
		Dataset<Territories> res_Territories;
		
		
		//Join datasets or return 
		Dataset<Territories> res = fullOuterJoinsTerritories(datasetsPOJO);
		if(res == null)
			return null;
	
		if(territories_refilter.booleanValue())
			res = res.filter((FilterFunction<Territories>) r -> territories_condition == null || territories_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Territories> getTerritoriesListInWorks(conditions.Condition<conditions.EmployeesAttribute> employed_condition,conditions.Condition<conditions.TerritoriesAttribute> territories_condition)		{
		MutableBoolean territories_refilter = new MutableBoolean(false);
		List<Dataset<Territories>> datasetsPOJO = new ArrayList<Dataset<Territories>>();
		Dataset<Employees> all = null;
		boolean all_already_persisted = false;
		MutableBoolean employed_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		// join physical structure A<-AB->B
		
		//join between 3 SQL tables stored in same db
		// (A AB B)
		employed_refilter = new MutableBoolean(false);
		MutableBoolean works_refilter = new MutableBoolean(false);
		Dataset<Works> res_Territories_territory_employee = worksService.getWorksListInTerritoriesAndEmployeeTerritoriesAndEmployeesFrommyRelDB(territories_condition, employed_condition, territories_refilter, employed_refilter);
		Dataset<Territories> res_Territories_territory = null;
		
		if(employed_refilter.booleanValue()) {
			if(all == null)
					all = new EmployeesServiceImpl().getEmployeesList(employed_condition);
			joinCondition = null;
				joinCondition = res_Territories_territory_employee.col("employed.employeeID").equalTo(all.col("employeeID"));
				if(joinCondition == null)
					res_Territories_territory = res_Territories_territory_employee.join(all).select("territories.*").as(Encoders.bean(Territories.class));
				else
					res_Territories_territory = res_Territories_territory_employee.join(all, joinCondition).select("territories.*").as(Encoders.bean(Territories.class));
		} else
			res_Territories_territory = res_Territories_territory_employee.map((MapFunction<Works,Territories>) r -> r.getTerritories(), Encoders.bean(Territories.class));
			
		datasetsPOJO.add(res_Territories_territory.dropDuplicates(new String[] {"territoryID"}));	
		
		
		
		Dataset<Works> res_works_territories;
		Dataset<Territories> res_Territories;
		
		
		//Join datasets or return 
		Dataset<Territories> res = fullOuterJoinsTerritories(datasetsPOJO);
		if(res == null)
			return null;
	
		if(territories_refilter.booleanValue())
			res = res.filter((FilterFunction<Territories>) r -> territories_condition == null || territories_condition.evaluate(r));
		
	
		return res;
		}
	
	public boolean insertTerritories(
		Territories territories,
		Region	regionLocatedIn){
			boolean inserted = false;
			// Insert in standalone structures
			// Insert in structures containing double embedded role
			// Insert in descending structures
			// Insert in ascending structures 
			// Insert in ref structures 
			inserted = insertTerritoriesInTerritoriesFromMyRelDB(territories,regionLocatedIn)|| inserted ;
			// Insert in ref structures mapped to opposite role of mandatory role  
			return inserted;
		}
	
	public boolean insertTerritoriesInTerritoriesFromMyRelDB(Territories territories,
		Region	regionLocatedIn)	{
			 // Implement Insert in structures with mandatory references
			List<String> columns = new ArrayList<>();
			List<Object> values = new ArrayList<>();
			List<List<Object>> rows = new ArrayList<>();
			Object territoriesId;
		columns.add("TerritoryID");
		values.add(territories.getTerritoryID());
		columns.add("TerritoryDescription");
		values.add(territories.getTerritoryDescription());
			// Ref 'located' mapped to role 'territories'
			columns.add("RegionRef");
			values.add(regionLocatedIn.getRegionID());
			rows.add(values);
			DBConnectionMgr.insertInTable(columns, rows, "Territories", "myRelDB");
			return true;
		
		}
	private boolean inUpdateMethod = false;
	private List<Row> allTerritoriesIdList = null;
	public void updateTerritoriesList(conditions.Condition<conditions.TerritoriesAttribute> condition, conditions.SetClause<conditions.TerritoriesAttribute> set){
		inUpdateMethod = true;
		try {
	
	
		} finally {
			inUpdateMethod = false;
		}
	}
	
	
	
	
	
	public void updateTerritories(pojo.Territories territories) {
		//TODO using the id
		return;
	}
	public void updateTerritoriesListInLocatedIn(
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition,
		conditions.Condition<conditions.RegionAttribute> region_condition,
		
		conditions.SetClause<conditions.TerritoriesAttribute> set
	){
		//TODO
	}
	
	public void updateTerritoriesListInLocatedInByTerritoriesCondition(
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition,
		conditions.SetClause<conditions.TerritoriesAttribute> set
	){
		updateTerritoriesListInLocatedIn(territories_condition, null, set);
	}
	public void updateTerritoriesListInLocatedInByRegionCondition(
		conditions.Condition<conditions.RegionAttribute> region_condition,
		conditions.SetClause<conditions.TerritoriesAttribute> set
	){
		updateTerritoriesListInLocatedIn(null, region_condition, set);
	}
	
	public void updateTerritoriesListInLocatedInByRegion(
		pojo.Region region,
		conditions.SetClause<conditions.TerritoriesAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateTerritoriesListInWorks(
		conditions.Condition<conditions.EmployeesAttribute> employed_condition,
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition,
		
		conditions.SetClause<conditions.TerritoriesAttribute> set
	){
		//TODO
	}
	
	public void updateTerritoriesListInWorksByEmployedCondition(
		conditions.Condition<conditions.EmployeesAttribute> employed_condition,
		conditions.SetClause<conditions.TerritoriesAttribute> set
	){
		updateTerritoriesListInWorks(employed_condition, null, set);
	}
	
	public void updateTerritoriesListInWorksByEmployed(
		pojo.Employees employed,
		conditions.SetClause<conditions.TerritoriesAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateTerritoriesListInWorksByTerritoriesCondition(
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition,
		conditions.SetClause<conditions.TerritoriesAttribute> set
	){
		updateTerritoriesListInWorks(null, territories_condition, set);
	}
	
	
	public void deleteTerritoriesList(conditions.Condition<conditions.TerritoriesAttribute> condition){
		//TODO
	}
	
	public void deleteTerritories(pojo.Territories territories) {
		//TODO using the id
		return;
	}
	public void deleteTerritoriesListInLocatedIn(	
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition,	
		conditions.Condition<conditions.RegionAttribute> region_condition){
			//TODO
		}
	
	public void deleteTerritoriesListInLocatedInByTerritoriesCondition(
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition
	){
		deleteTerritoriesListInLocatedIn(territories_condition, null);
	}
	public void deleteTerritoriesListInLocatedInByRegionCondition(
		conditions.Condition<conditions.RegionAttribute> region_condition
	){
		deleteTerritoriesListInLocatedIn(null, region_condition);
	}
	
	public void deleteTerritoriesListInLocatedInByRegion(
		pojo.Region region 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteTerritoriesListInWorks(	
		conditions.Condition<conditions.EmployeesAttribute> employed_condition,	
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition){
			//TODO
		}
	
	public void deleteTerritoriesListInWorksByEmployedCondition(
		conditions.Condition<conditions.EmployeesAttribute> employed_condition
	){
		deleteTerritoriesListInWorks(employed_condition, null);
	}
	
	public void deleteTerritoriesListInWorksByEmployed(
		pojo.Employees employed 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteTerritoriesListInWorksByTerritoriesCondition(
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition
	){
		deleteTerritoriesListInWorks(null, territories_condition);
	}
	
}
