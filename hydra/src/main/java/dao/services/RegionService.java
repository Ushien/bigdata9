package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import pojo.Region;
import conditions.RegionAttribute;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.commons.lang.mutable.MutableBoolean;
import conditions.Condition;
import conditions.Operator;
import util.Util;
import conditions.RegionAttribute;
import pojo.LocatedIn;
import conditions.TerritoriesAttribute;
import pojo.Territories;

public abstract class RegionService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RegionService.class);
	protected LocatedInService locatedInService = new dao.impl.LocatedInServiceImpl();
	


	public static enum ROLE_NAME {
		LOCATEDIN_REGION
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.LOCATEDIN_REGION, loading.Loading.LAZY);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public RegionService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public RegionService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
		this();
		if(loadingParams != null)
			for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: loadingParams.entrySet())
				loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public static java.util.Map<ROLE_NAME, loading.Loading> getDefaultLoadingParameters() {
		java.util.Map<ROLE_NAME, loading.Loading> res = new java.util.HashMap<ROLE_NAME, loading.Loading>();
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
				res.put(entry.getKey(), entry.getValue());
		return res;
	}
	
	public static void setAllDefaultLoadingParameters(loading.Loading loading) {
		java.util.Map<ROLE_NAME, loading.Loading> newParams = new java.util.HashMap<ROLE_NAME, loading.Loading>();
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
				newParams.put(entry.getKey(), entry.getValue());
		defaultLoadingParameters = newParams;
	}
	
	public java.util.Map<ROLE_NAME, loading.Loading> getLoadingParameters() {
		return this.loadingParameters;
	}
	
	public void setLoadingParameters(java.util.Map<ROLE_NAME, loading.Loading> newParams) {
		this.loadingParameters = newParams;
	}
	
	public void updateLoadingParameter(ROLE_NAME role, loading.Loading l) {
		this.loadingParameters.put(role, l);
	}
	
	
	public Dataset<Region> getRegionList(){
		return getRegionList(null);
	}
	
	public Dataset<Region> getRegionList(conditions.Condition<conditions.RegionAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Region>> datasets = new ArrayList<Dataset<Region>>();
		Dataset<Region> d = null;
		d = getRegionListInRegionFromMyRelDB(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsRegion(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Region>) r -> condition == null || condition.evaluate(r));
		d = d.dropDuplicates(new String[] {"regionID"});
		return d;
	}
	
	
	
	
	
	public abstract Dataset<Region> getRegionListInRegionFromMyRelDB(conditions.Condition<conditions.RegionAttribute> condition, MutableBoolean refilterFlag);
	
	
	public Region getRegionById(Integer regionID){
		Condition cond;
		cond = Condition.simple(RegionAttribute.regionID, conditions.Operator.EQUALS, regionID);
		Dataset<Region> res = getRegionList(cond);
		if(res!=null && !res.isEmpty())
			return res.first();
		return null;
	}
	
	public Dataset<Region> getRegionListByRegionID(Integer regionID) {
		return getRegionList(conditions.Condition.simple(conditions.RegionAttribute.regionID, conditions.Operator.EQUALS, regionID));
	}
	
	public Dataset<Region> getRegionListByRegionDescription(String regionDescription) {
		return getRegionList(conditions.Condition.simple(conditions.RegionAttribute.regionDescription, conditions.Operator.EQUALS, regionDescription));
	}
	
	
	
	public static Dataset<Region> fullOuterJoinsRegion(List<Dataset<Region>> datasetsPOJO) {
		return fullOuterJoinsRegion(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Region> fullLeftOuterJoinsRegion(List<Dataset<Region>> datasetsPOJO) {
		return fullOuterJoinsRegion(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Region> fullOuterJoinsRegion(List<Dataset<Region>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Region> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("regionID");
			logger.debug("Start {} of [{}] datasets of [Region] objects",joinMode,datasetsPOJO.size());
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("regionDescription", "regionDescription_1")
								.withColumnRenamed("logEvents", "logEvents_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("regionDescription", "regionDescription_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
						, seq, joinMode);
			}
			logger.debug("End join. Start");
			logger.debug("Start transforming Row objects to [Region] objects"); 
			d = res.map((MapFunction<Row, Region>) r -> {
					Region region_res = new Region();
					
					// attribute 'Region.regionID'
					Integer firstNotNull_regionID = Util.getIntegerValue(r.getAs("regionID"));
					region_res.setRegionID(firstNotNull_regionID);
					
					// attribute 'Region.regionDescription'
					String firstNotNull_regionDescription = Util.getStringValue(r.getAs("regionDescription"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String regionDescription2 = Util.getStringValue(r.getAs("regionDescription_" + i));
						if (firstNotNull_regionDescription != null && regionDescription2 != null && !firstNotNull_regionDescription.equals(regionDescription2)) {
							region_res.addLogEvent("Data consistency problem for [Region - id :"+region_res.getRegionID()+"]: different values found for attribute 'Region.regionDescription': " + firstNotNull_regionDescription + " and " + regionDescription2 + "." );
							logger.warn("Data consistency problem for [Region - id :"+region_res.getRegionID()+"]: different values found for attribute 'Region.regionDescription': " + firstNotNull_regionDescription + " and " + regionDescription2 + "." );
						}
						if (firstNotNull_regionDescription == null && regionDescription2 != null) {
							firstNotNull_regionDescription = regionDescription2;
						}
					}
					region_res.setRegionDescription(firstNotNull_regionDescription);
	
					WrappedArray logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							region_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							region_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					return region_res;
				}, Encoders.bean(Region.class));
			return d;
	}
	
	
	public Region getRegion(Region.locatedIn role, Territories territories) {
		if(role != null) {
			if(role.equals(Region.locatedIn.region))
				return getRegionInLocatedInByTerritories(territories);
		}
		return null;
	}
	
	public Dataset<Region> getRegionList(Region.locatedIn role, Condition<TerritoriesAttribute> condition) {
		if(role != null) {
			if(role.equals(Region.locatedIn.region))
				return getRegionListInLocatedInByTerritoriesCondition(condition);
		}
		return null;
	}
	
	public Dataset<Region> getRegionList(Region.locatedIn role, Condition<TerritoriesAttribute> condition1, Condition<RegionAttribute> condition2) {
		if(role != null) {
			if(role.equals(Region.locatedIn.region))
				return getRegionListInLocatedIn(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	
	
	
	
	
	
	
	public abstract Dataset<Region> getRegionListInLocatedIn(conditions.Condition<conditions.TerritoriesAttribute> territories_condition,conditions.Condition<conditions.RegionAttribute> region_condition);
	
	public Dataset<Region> getRegionListInLocatedInByTerritoriesCondition(conditions.Condition<conditions.TerritoriesAttribute> territories_condition){
		return getRegionListInLocatedIn(territories_condition, null);
	}
	
	public Region getRegionInLocatedInByTerritories(pojo.Territories territories){
		if(territories == null)
			return null;
	
		Condition c;
		c=Condition.simple(TerritoriesAttribute.territoryID,Operator.EQUALS, territories.getTerritoryID());
		Dataset<Region> res = getRegionListInLocatedInByTerritoriesCondition(c);
		return !res.isEmpty()?res.first():null;
	}
	
	public Dataset<Region> getRegionListInLocatedInByRegionCondition(conditions.Condition<conditions.RegionAttribute> region_condition){
		return getRegionListInLocatedIn(null, region_condition);
	}
	
	
	public abstract boolean insertRegion(Region region);
	
	public abstract boolean insertRegionInRegionFromMyRelDB(Region region); 
	private boolean inUpdateMethod = false;
	private List<Row> allRegionIdList = null;
	public abstract void updateRegionList(conditions.Condition<conditions.RegionAttribute> condition, conditions.SetClause<conditions.RegionAttribute> set);
	
	public void updateRegion(pojo.Region region) {
		//TODO using the id
		return;
	}
	public abstract void updateRegionListInLocatedIn(
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition,
		conditions.Condition<conditions.RegionAttribute> region_condition,
		
		conditions.SetClause<conditions.RegionAttribute> set
	);
	
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
	
	
	public abstract void deleteRegionList(conditions.Condition<conditions.RegionAttribute> condition);
	
	public void deleteRegion(pojo.Region region) {
		//TODO using the id
		return;
	}
	public abstract void deleteRegionListInLocatedIn(	
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition,	
		conditions.Condition<conditions.RegionAttribute> region_condition);
	
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
