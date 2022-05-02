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
import conditions.LocatedInAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.TerritoriesTDO;
import tdo.LocatedInTDO;
import conditions.TerritoriesAttribute;
import dao.services.TerritoriesService;
import tdo.RegionTDO;
import tdo.LocatedInTDO;
import conditions.RegionAttribute;
import dao.services.RegionService;
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

public class LocatedInServiceImpl extends dao.services.LocatedInService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LocatedInServiceImpl.class);
	
	public  Dataset<LocatedIn> getTerritoriesAndRegionListInLocatedInTerritoriesFromMyRelDB(Condition<TerritoriesAttribute> territories_cond, Condition<RegionAttribute> region_cond, MutableBoolean territories_refilter, MutableBoolean region_refilter) {
		Pair<String, List<String>> whereClause1 = TerritoriesServiceImpl.getSQLWhereClauseInTerritoriesFromMyRelDBWithTableAlias(territories_cond, territories_refilter, "TerritoriesA.");
		Pair<String, List<String>> whereClause2 = RegionServiceImpl.getSQLWhereClauseInRegionFromMyRelDBWithTableAlias(region_cond, region_refilter, "RegionB.");
		
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
		String aliasedColumns = "TerritoriesA.TerritoryID as TerritoriesA_TerritoryID,TerritoriesA.TerritoryDescription as TerritoriesA_TerritoryDescription,TerritoriesA.RegionRef as TerritoriesA_RegionRef,RegionB.RegionID as RegionB_RegionID,RegionB.RegionDescription as RegionB_RegionDescription";
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("myRelDB", "(SELECT " + aliasedColumns + " FROM Territories TerritoriesA, Region RegionB WHERE TerritoriesA.RegionRef = RegionB.RegionID" + where + ") AS JOIN_TABLE", null);
		
		Dataset<LocatedIn> res = d.map((MapFunction<Row, LocatedIn>) r -> {
					LocatedIn locatedIn_res = new LocatedIn();
					locatedIn_res.setTerritories(new Territories());
					locatedIn_res.setRegion(new Region());
					
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
	
	
					
					// attribute [Territories.TerritoryID]
					String territories_territoryID = Util.getStringValue(r.getAs("TerritoriesA_TerritoryID"));
					locatedIn_res.getTerritories().setTerritoryID(territories_territoryID);
					
					// attribute [Territories.TerritoryDescription]
					String territories_territoryDescription = Util.getStringValue(r.getAs("TerritoriesA_TerritoryDescription"));
					locatedIn_res.getTerritories().setTerritoryDescription(territories_territoryDescription);
	
					
					// attribute [Region.RegionID]
					Integer region_regionID = Util.getIntegerValue(r.getAs("RegionB_RegionID"));
					locatedIn_res.getRegion().setRegionID(region_regionID);
					
					// attribute [Region.RegionDescription]
					String region_regionDescription = Util.getStringValue(r.getAs("RegionB_RegionDescription"));
					locatedIn_res.getRegion().setRegionDescription(region_regionDescription);
	
					return locatedIn_res;
				}, Encoders.bean(LocatedIn.class));
	
		return res;
	}
	
	
	
	
	
	public Dataset<LocatedIn> getLocatedInList(
		Condition<TerritoriesAttribute> territories_condition,
		Condition<RegionAttribute> region_condition){
			LocatedInServiceImpl locatedInService = this;
			TerritoriesService territoriesService = new TerritoriesServiceImpl();  
			RegionService regionService = new RegionServiceImpl();
			MutableBoolean territories_refilter = new MutableBoolean(false);
			List<Dataset<LocatedIn>> datasetsPOJO = new ArrayList<Dataset<LocatedIn>>();
			boolean all_already_persisted = false;
			MutableBoolean region_refilter = new MutableBoolean(false);
			
			org.apache.spark.sql.Column joinCondition = null;
			// For role 'territories' in reference 'located'. A->B Scenario in SQL DB
			region_refilter = new MutableBoolean(false);
			Dataset<LocatedIn> res_located = locatedInService.getTerritoriesAndRegionListInLocatedInTerritoriesFromMyRelDB(territories_condition, region_condition, territories_refilter, region_refilter);
			datasetsPOJO.add(res_located);
		
			
			Dataset<LocatedIn> res_locatedIn_territories;
			Dataset<Territories> res_Territories;
			
			
			//Join datasets or return 
			Dataset<LocatedIn> res = fullOuterJoinsLocatedIn(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Territories> lonelyTerritories = null;
			Dataset<Region> lonelyRegion = null;
			
		
		
			
			if(territories_refilter.booleanValue() || region_refilter.booleanValue())
				res = res.filter((FilterFunction<LocatedIn>) r -> (territories_condition == null || territories_condition.evaluate(r.getTerritories())) && (region_condition == null || region_condition.evaluate(r.getRegion())));
			
		
			return res;
		
		}
	
	public Dataset<LocatedIn> getLocatedInListByTerritoriesCondition(
		Condition<TerritoriesAttribute> territories_condition
	){
		return getLocatedInList(territories_condition, null);
	}
	
	public LocatedIn getLocatedInByTerritories(Territories territories) {
		Condition<TerritoriesAttribute> cond = null;
		cond = Condition.simple(TerritoriesAttribute.territoryID, Operator.EQUALS, territories.getTerritoryID());
		Dataset<LocatedIn> res = getLocatedInListByTerritoriesCondition(cond);
		List<LocatedIn> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	public Dataset<LocatedIn> getLocatedInListByRegionCondition(
		Condition<RegionAttribute> region_condition
	){
		return getLocatedInList(null, region_condition);
	}
	
	public Dataset<LocatedIn> getLocatedInListByRegion(Region region) {
		Condition<RegionAttribute> cond = null;
		cond = Condition.simple(RegionAttribute.regionID, Operator.EQUALS, region.getRegionID());
		Dataset<LocatedIn> res = getLocatedInListByRegionCondition(cond);
	return res;
	}
	
	
	
	public void deleteLocatedInList(
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition,
		conditions.Condition<conditions.RegionAttribute> region_condition){
			//TODO
		}
	
	public void deleteLocatedInListByTerritoriesCondition(
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition
	){
		deleteLocatedInList(territories_condition, null);
	}
	
	public void deleteLocatedInByTerritories(pojo.Territories territories) {
		// TODO using id for selecting
		return;
	}
	public void deleteLocatedInListByRegionCondition(
		conditions.Condition<conditions.RegionAttribute> region_condition
	){
		deleteLocatedInList(null, region_condition);
	}
	
	public void deleteLocatedInListByRegion(pojo.Region region) {
		// TODO using id for selecting
		return;
	}
		
}
