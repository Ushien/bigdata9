package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import conditions.*;
import pojo.LocatedIn;
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


public abstract class LocatedInService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LocatedInService.class);
	
	public abstract Dataset<LocatedIn> getTerritoriesAndRegionListInLocatedInTerritoriesFromMyRelDB(Condition<TerritoriesAttribute> territories_cond, Condition<RegionAttribute> region_cond, MutableBoolean territories_refilter, MutableBoolean region_refilter);
	
	
	
	
	
	public static Dataset<LocatedIn> fullLeftOuterJoinBetweenLocatedInAndTerritories(Dataset<LocatedIn> d1, Dataset<Territories> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("territoryID", "A_territoryID")
			.withColumnRenamed("territoryDescription", "A_territoryDescription")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("territories.territoryID").equalTo(d2_.col("A_territoryID"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, LocatedIn>) r -> {
				LocatedIn res = new LocatedIn();
	
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
					res.addLogEvent("Data consistency problem for [LocatedIn - different values found for attribute 'LocatedIn.territoryID': " + territories.getTerritoryID() + " and " + territoryID + "." );
					logger.warn("Data consistency problem for [LocatedIn - different values found for attribute 'LocatedIn.territoryID': " + territories.getTerritoryID() + " and " + territoryID + "." );
				}
				if(territoryID != null)
					territories.setTerritoryID(territoryID);
				String territoryDescription = Util.getStringValue(r.getAs("A_territoryDescription"));
				if (territories.getTerritoryDescription() != null && territoryDescription != null && !territories.getTerritoryDescription().equals(territoryDescription)) {
					res.addLogEvent("Data consistency problem for [LocatedIn - different values found for attribute 'LocatedIn.territoryDescription': " + territories.getTerritoryDescription() + " and " + territoryDescription + "." );
					logger.warn("Data consistency problem for [LocatedIn - different values found for attribute 'LocatedIn.territoryDescription': " + territories.getTerritoryDescription() + " and " + territoryDescription + "." );
				}
				if(territoryDescription != null)
					territories.setTerritoryDescription(territoryDescription);
	
				o = r.getAs("region");
				Region region = new Region();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						region.setRegionID(Util.getIntegerValue(r2.getAs("regionID")));
						region.setRegionDescription(Util.getStringValue(r2.getAs("regionDescription")));
					} 
					if(o instanceof Region) {
						region = (Region) o;
					}
				}
	
				res.setRegion(region);
	
				return res;
		}, Encoders.bean(LocatedIn.class));
	
		
		
	}
	public static Dataset<LocatedIn> fullLeftOuterJoinBetweenLocatedInAndRegion(Dataset<LocatedIn> d1, Dataset<Region> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("regionID", "A_regionID")
			.withColumnRenamed("regionDescription", "A_regionDescription")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("region.regionID").equalTo(d2_.col("A_regionID"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, LocatedIn>) r -> {
				LocatedIn res = new LocatedIn();
	
				Region region = new Region();
				Object o = r.getAs("region");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						region.setRegionID(Util.getIntegerValue(r2.getAs("regionID")));
						region.setRegionDescription(Util.getStringValue(r2.getAs("regionDescription")));
					} 
					if(o instanceof Region) {
						region = (Region) o;
					}
				}
	
				res.setRegion(region);
	
				Integer regionID = Util.getIntegerValue(r.getAs("A_regionID"));
				if (region.getRegionID() != null && regionID != null && !region.getRegionID().equals(regionID)) {
					res.addLogEvent("Data consistency problem for [LocatedIn - different values found for attribute 'LocatedIn.regionID': " + region.getRegionID() + " and " + regionID + "." );
					logger.warn("Data consistency problem for [LocatedIn - different values found for attribute 'LocatedIn.regionID': " + region.getRegionID() + " and " + regionID + "." );
				}
				if(regionID != null)
					region.setRegionID(regionID);
				String regionDescription = Util.getStringValue(r.getAs("A_regionDescription"));
				if (region.getRegionDescription() != null && regionDescription != null && !region.getRegionDescription().equals(regionDescription)) {
					res.addLogEvent("Data consistency problem for [LocatedIn - different values found for attribute 'LocatedIn.regionDescription': " + region.getRegionDescription() + " and " + regionDescription + "." );
					logger.warn("Data consistency problem for [LocatedIn - different values found for attribute 'LocatedIn.regionDescription': " + region.getRegionDescription() + " and " + regionDescription + "." );
				}
				if(regionDescription != null)
					region.setRegionDescription(regionDescription);
	
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
		}, Encoders.bean(LocatedIn.class));
	
		
		
	}
	
	public static Dataset<LocatedIn> fullOuterJoinsLocatedIn(List<Dataset<LocatedIn>> datasetsPOJO) {
		return fullOuterJoinsLocatedIn(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<LocatedIn> fullLeftOuterJoinsLocatedIn(List<Dataset<LocatedIn>> datasetsPOJO) {
		return fullOuterJoinsLocatedIn(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<LocatedIn> fullOuterJoinsLocatedIn(List<Dataset<LocatedIn>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		List<String> idFields = new ArrayList<String>();
		idFields.add("territories.territoryID");
	
		idFields.add("region.regionID");
		scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
		
		List<Dataset<Row>> rows = new ArrayList<Dataset<Row>>();
		for(int i = 0; i < datasetsPOJO.size(); i++) {
			Dataset<LocatedIn> d = datasetsPOJO.get(i);
			rows.add(d
				.withColumn("territories_territoryID_" + i, d.col("territories.territoryID"))
				.withColumn("region_regionID_" + i, d.col("region.regionID"))
				.withColumnRenamed("territories", "territories_" + i)
				.withColumnRenamed("region", "region_" + i)
				.withColumnRenamed("logEvents", "logEvents_" + i));
		}
		
		Column joinCond;
		joinCond = rows.get(0).col("territories_territoryID_0").equalTo(rows.get(1).col("territories_territoryID_1"));
		joinCond = joinCond.and(rows.get(0).col("region_regionID_0").equalTo(rows.get(1).col("region_regionID_1")));
		
		Dataset<Row> res = rows.get(0).join(rows.get(1), joinCond, joinMode);
		for(int i = 2; i < rows.size(); i++) {
			joinCond = rows.get(i - 1).col("territories_territoryID_" + (i - 1)).equalTo(rows.get(i).col("territories_territoryID_" + i));
			joinCond = joinCond.and(rows.get(i - 1).col("region_regionID_" + (i - 1)).equalTo(rows.get(i).col("region_regionID_" + i)));
			res = res.join(rows.get(i), joinCond, joinMode);
		}
	
		return res.map((MapFunction<Row, LocatedIn>) r -> {
				LocatedIn locatedIn_res = new LocatedIn();
	
					WrappedArray logEvents = r.getAs("logEvents_0");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							locatedIn_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							locatedIn_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					Territories territories_res = new Territories();
					Region region_res = new Region();
					
					// attribute 'Territories.territoryID'
					String firstNotNull_territories_territoryID = Util.getStringValue(r.getAs("territories_0.territoryID"));
					territories_res.setTerritoryID(firstNotNull_territories_territoryID);
					// attribute 'Territories.territoryDescription'
					String firstNotNull_territories_TerritoryDescription = Util.getStringValue(r.getAs("territories_0.territoryDescription"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String territories_territoryDescription2 = Util.getStringValue(r.getAs("territories_" + i + ".territoryDescription"));
						if (firstNotNull_territories_TerritoryDescription != null && territories_territoryDescription2 != null && !firstNotNull_territories_TerritoryDescription.equals(territories_territoryDescription2)) {
							locatedIn_res.addLogEvent("Data consistency problem for [Territories - id :"+territories_res.getTerritoryID()+"]: different values found for attribute 'Territories.territoryDescription': " + firstNotNull_territories_TerritoryDescription + " and " + territories_territoryDescription2 + "." );
							logger.warn("Data consistency problem for [Territories - id :"+territories_res.getTerritoryID()+"]: different values found for attribute 'Territories.territoryDescription': " + firstNotNull_territories_TerritoryDescription + " and " + territories_territoryDescription2 + "." );
						}
						if (firstNotNull_territories_TerritoryDescription == null && territories_territoryDescription2 != null) {
							firstNotNull_territories_TerritoryDescription = territories_territoryDescription2;
						}
					}
					territories_res.setTerritoryDescription(firstNotNull_territories_TerritoryDescription);
					// attribute 'Region.regionID'
					Integer firstNotNull_region_regionID = Util.getIntegerValue(r.getAs("region_0.regionID"));
					region_res.setRegionID(firstNotNull_region_regionID);
					// attribute 'Region.regionDescription'
					String firstNotNull_region_RegionDescription = Util.getStringValue(r.getAs("region_0.regionDescription"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String region_regionDescription2 = Util.getStringValue(r.getAs("region_" + i + ".regionDescription"));
						if (firstNotNull_region_RegionDescription != null && region_regionDescription2 != null && !firstNotNull_region_RegionDescription.equals(region_regionDescription2)) {
							locatedIn_res.addLogEvent("Data consistency problem for [Region - id :"+region_res.getRegionID()+"]: different values found for attribute 'Region.regionDescription': " + firstNotNull_region_RegionDescription + " and " + region_regionDescription2 + "." );
							logger.warn("Data consistency problem for [Region - id :"+region_res.getRegionID()+"]: different values found for attribute 'Region.regionDescription': " + firstNotNull_region_RegionDescription + " and " + region_regionDescription2 + "." );
						}
						if (firstNotNull_region_RegionDescription == null && region_regionDescription2 != null) {
							firstNotNull_region_RegionDescription = region_regionDescription2;
						}
					}
					region_res.setRegionDescription(firstNotNull_region_RegionDescription);
	
					locatedIn_res.setTerritories(territories_res);
					locatedIn_res.setRegion(region_res);
					return locatedIn_res;
		}
		, Encoders.bean(LocatedIn.class));
	
	}
	
	//Empty arguments
	public Dataset<LocatedIn> getLocatedInList(){
		 return getLocatedInList(null,null);
	}
	
	public abstract Dataset<LocatedIn> getLocatedInList(
		Condition<TerritoriesAttribute> territories_condition,
		Condition<RegionAttribute> region_condition);
	
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
	
	
	
	public abstract void deleteLocatedInList(
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition,
		conditions.Condition<conditions.RegionAttribute> region_condition);
	
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
