package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Region;
import conditions.*;
import dao.services.RegionService;
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


public class RegionServiceImpl extends RegionService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RegionServiceImpl.class);
	
	
	
	
	public static Pair<String, List<String>> getSQLWhereClauseInRegionFromMyRelDB(Condition<RegionAttribute> condition, MutableBoolean refilterFlag) {
		return getSQLWhereClauseInRegionFromMyRelDBWithTableAlias(condition, refilterFlag, "");
	}
	
	public static List<String> getSQLSetClauseInRegionFromMyRelDB(conditions.SetClause<RegionAttribute> set) {
		List<String> res = new ArrayList<String>();
		if(set != null) {
			java.util.Map<String, java.util.Map<String, String>> longFieldValues = new java.util.HashMap<String, java.util.Map<String, String>>();
			java.util.Map<RegionAttribute, Object> clause = set.getClause();
			for(java.util.Map.Entry<RegionAttribute, Object> e : clause.entrySet()) {
				RegionAttribute attr = e.getKey();
				Object value = e.getValue();
				if(attr == RegionAttribute.regionID ) {
					res.add("RegionID = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == RegionAttribute.regionDescription ) {
					res.add("RegionDescription = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
			}
	
			for(java.util.Map.Entry<String, java.util.Map<String, String>> entry : longFieldValues.entrySet()) {
				String longField = entry.getKey();
				java.util.Map<String, String> values = entry.getValue();
			}
	
		}
		return res;
	}
	
	public static Pair<String, List<String>> getSQLWhereClauseInRegionFromMyRelDBWithTableAlias(Condition<RegionAttribute> condition, MutableBoolean refilterFlag, String tableAlias) {
		String where = null;	
		List<String> preparedValues = new java.util.ArrayList<String>();
		if(condition != null) {
			
			if(condition instanceof SimpleCondition) {
				RegionAttribute attr = ((SimpleCondition<RegionAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<RegionAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<RegionAttribute>) condition).getValue();
				if(value != null) {
					boolean isConditionAttrEncountered = false;
					if(attr == RegionAttribute.regionID ) {
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
						
						where = tableAlias + "RegionID " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == RegionAttribute.regionDescription ) {
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
						
						where = tableAlias + "RegionDescription " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						where = "1 = 1";
					}
				} else {
					if(attr == RegionAttribute.regionID ) {
						if(op == Operator.EQUALS)
							where =  "RegionID IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "RegionID IS NOT NULL";
					}
					if(attr == RegionAttribute.regionDescription ) {
						if(op == Operator.EQUALS)
							where =  "RegionDescription IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "RegionDescription IS NOT NULL";
					}
				}
			}
	
			if(condition instanceof AndCondition) {
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInRegionFromMyRelDB(((AndCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInRegionFromMyRelDB(((AndCondition) condition).getRightCondition(), refilterFlag);
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
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInRegionFromMyRelDB(((OrCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInRegionFromMyRelDB(((OrCondition) condition).getRightCondition(), refilterFlag);
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
	
	
	
	public Dataset<Region> getRegionListInRegionFromMyRelDB(conditions.Condition<conditions.RegionAttribute> condition, MutableBoolean refilterFlag){
	
		Pair<String, List<String>> whereClause = RegionServiceImpl.getSQLWhereClauseInRegionFromMyRelDB(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", preparedValue);
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("myRelDB", "Region", where);
		
	
		Dataset<Region> res = d.map((MapFunction<Row, Region>) r -> {
					Region region_res = new Region();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
					// attribute [Region.RegionID]
					Integer regionID = Util.getIntegerValue(r.getAs("RegionID"));
					region_res.setRegionID(regionID);
					
					// attribute [Region.RegionDescription]
					String regionDescription = Util.getStringValue(r.getAs("RegionDescription"));
					region_res.setRegionDescription(regionDescription);
	
	
	
					return region_res;
				}, Encoders.bean(Region.class));
	
	
		return res;
		
	}
	
	
	
	
	
	
	public Dataset<Region> getRegionListInLocatedIn(conditions.Condition<conditions.TerritoriesAttribute> territories_condition,conditions.Condition<conditions.RegionAttribute> region_condition)		{
		MutableBoolean region_refilter = new MutableBoolean(false);
		List<Dataset<Region>> datasetsPOJO = new ArrayList<Dataset<Region>>();
		Dataset<Territories> all = null;
		boolean all_already_persisted = false;
		MutableBoolean territories_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		territories_refilter = new MutableBoolean(false);
		// For role 'territories' in reference 'located'  B->A Scenario in SQL Db.
		Dataset<LocatedIn> res_located = locatedInService.getTerritoriesAndRegionListInLocatedInTerritoriesFromMyRelDB(territories_condition, region_condition, territories_refilter, region_refilter);
		Dataset<Region> res_Region_located = null;
		if(territories_refilter.booleanValue()) {
			if(all == null)
				all = new TerritoriesServiceImpl().getTerritoriesList(territories_condition);
			joinCondition = null;
			joinCondition = res_located.col("territories.territoryID").equalTo(all.col("territoryID"));
			if(joinCondition == null)
				res_Region_located = res_located.join(all).select("region.*").as(Encoders.bean(Region.class));
			else
				res_Region_located = res_located.join(all, joinCondition).select("region.*").as(Encoders.bean(Region.class));
		} else
			res_Region_located = res_located.map((MapFunction<LocatedIn,Region>) r -> r.getRegion(), Encoders.bean(Region.class));
		res_Region_located = res_Region_located.dropDuplicates(new String[] {"regionID"});
		datasetsPOJO.add(res_Region_located);
		
		Dataset<LocatedIn> res_locatedIn_region;
		Dataset<Region> res_Region;
		
		
		//Join datasets or return 
		Dataset<Region> res = fullOuterJoinsRegion(datasetsPOJO);
		if(res == null)
			return null;
	
		if(region_refilter.booleanValue())
			res = res.filter((FilterFunction<Region>) r -> region_condition == null || region_condition.evaluate(r));
		
	
		return res;
		}
	
	
	public boolean insertRegion(Region region){
		// Insert into all mapped standalone AbstractPhysicalStructure 
		boolean inserted = false;
			inserted = insertRegionInRegionFromMyRelDB(region) || inserted ;
		return inserted;
	}
	
	public boolean insertRegionInRegionFromMyRelDB(Region region)	{
		String idvalue="";
		idvalue+=region.getRegionID();
		boolean entityExists = false; // Modify in acceleo code (in 'main.services.insert.entitytype.generateSimpleInsertMethods.mtl') to generate checking before insert
		if(!entityExists){
		List<String> columns = new ArrayList<>();
		List<Object> values = new ArrayList<>();	
		columns.add("RegionID");
		values.add(region.getRegionID());
		columns.add("RegionDescription");
		values.add(region.getRegionDescription());
		DBConnectionMgr.insertInTable(columns, Arrays.asList(values), "Region", "myRelDB");
			logger.info("Inserted [Region] entity ID [{}] in [Region] in database [MyRelDB]", idvalue);
		}
		else
			logger.warn("[Region] entity ID [{}] already present in [Region] in database [MyRelDB]", idvalue);
		return !entityExists;
	} 
	
	private boolean inUpdateMethod = false;
	private List<Row> allRegionIdList = null;
	public void updateRegionList(conditions.Condition<conditions.RegionAttribute> condition, conditions.SetClause<conditions.RegionAttribute> set){
		inUpdateMethod = true;
		try {
			MutableBoolean refilterInRegionFromMyRelDB = new MutableBoolean(false);
			getSQLWhereClauseInRegionFromMyRelDB(condition, refilterInRegionFromMyRelDB);
			// one first updates in the structures necessitating to execute a "SELECT *" query to establish the update condition 
			if(refilterInRegionFromMyRelDB.booleanValue())
				updateRegionListInRegionFromMyRelDB(condition, set);
		
	
			if(!refilterInRegionFromMyRelDB.booleanValue())
				updateRegionListInRegionFromMyRelDB(condition, set);
	
		} finally {
			inUpdateMethod = false;
		}
	}
	
	
	public void updateRegionListInRegionFromMyRelDB(Condition<RegionAttribute> condition, SetClause<RegionAttribute> set) {
		List<String> setClause = RegionServiceImpl.getSQLSetClauseInRegionFromMyRelDB(set);
		String setSQL = null;
		for(int i = 0; i < setClause.size(); i++) {
			if(i == 0)
				setSQL = setClause.get(i);
			else
				setSQL += ", " + setClause.get(i);
		}
		
		if(setSQL == null)
			return;
		
		MutableBoolean refilter = new MutableBoolean(false);
		Pair<String, List<String>> whereClause = RegionServiceImpl.getSQLWhereClauseInRegionFromMyRelDB(condition, refilter);
		if(!refilter.booleanValue()) {
			String where = whereClause.getKey();
			List<String> preparedValues = whereClause.getValue();
			for(String preparedValue : preparedValues) {
				where = where.replaceFirst("\\?", preparedValue);
			}
			
			String sql = "UPDATE Region SET " + setSQL;
			if(where != null)
				sql += " WHERE " + where;
			
			DBConnectionMgr.updateInTable(sql, "myRelDB");
		} else {
			if(!inUpdateMethod || allRegionIdList == null)
				allRegionIdList = this.getRegionList(condition).select("regionID").collectAsList();
		
			List<String> updateQueries = new ArrayList<String>();
			for(Row row : allRegionIdList) {
				Condition<RegionAttribute> conditionId = null;
				conditionId = Condition.simple(RegionAttribute.regionID, Operator.EQUALS, row.getAs("regionID"));
				whereClause = RegionServiceImpl.getSQLWhereClauseInRegionFromMyRelDB(conditionId, refilter);
				String sql = "UPDATE Region SET " + setSQL;
				String where = whereClause.getKey();
				List<String> preparedValues = whereClause.getValue();
				for(String preparedValue : preparedValues) {
					where = where.replaceFirst("\\?", preparedValue);
				}
				if(where != null)
					sql += " WHERE " + where;
				updateQueries.add(sql);
			}
		
			DBConnectionMgr.updatesInTable(updateQueries, "myRelDB");
		}
		
	}
	
	
	
	public void updateRegion(pojo.Region region) {
		//TODO using the id
		return;
	}
	public void updateRegionListInLocatedIn(
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition,
		conditions.Condition<conditions.RegionAttribute> region_condition,
		
		conditions.SetClause<conditions.RegionAttribute> set
	){
		//TODO
	}
	
	public void updateRegionListInLocatedInByTerritoriesCondition(
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition,
		conditions.SetClause<conditions.RegionAttribute> set
	){
		updateRegionListInLocatedIn(territories_condition, null, set);
	}
	
	public void updateRegionInLocatedInByTerritories(
		pojo.Territories territories,
		conditions.SetClause<conditions.RegionAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateRegionListInLocatedInByRegionCondition(
		conditions.Condition<conditions.RegionAttribute> region_condition,
		conditions.SetClause<conditions.RegionAttribute> set
	){
		updateRegionListInLocatedIn(null, region_condition, set);
	}
	
	
	public void deleteRegionList(conditions.Condition<conditions.RegionAttribute> condition){
		//TODO
	}
	
	public void deleteRegion(pojo.Region region) {
		//TODO using the id
		return;
	}
	public void deleteRegionListInLocatedIn(	
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition,	
		conditions.Condition<conditions.RegionAttribute> region_condition){
			//TODO
		}
	
	public void deleteRegionListInLocatedInByTerritoriesCondition(
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition
	){
		deleteRegionListInLocatedIn(territories_condition, null);
	}
	
	public void deleteRegionInLocatedInByTerritories(
		pojo.Territories territories 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteRegionListInLocatedInByRegionCondition(
		conditions.Condition<conditions.RegionAttribute> region_condition
	){
		deleteRegionListInLocatedIn(null, region_condition);
	}
	
}
