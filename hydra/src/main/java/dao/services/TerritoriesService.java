package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import pojo.Territories;
import conditions.TerritoriesAttribute;
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
import conditions.TerritoriesAttribute;
import pojo.LocatedIn;
import conditions.RegionAttribute;
import pojo.Region;
import conditions.TerritoriesAttribute;
import pojo.Works;
import conditions.EmployeesAttribute;
import pojo.Employees;

public abstract class TerritoriesService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TerritoriesService.class);
	protected LocatedInService locatedInService = new dao.impl.LocatedInServiceImpl();
	protected WorksService worksService = new dao.impl.WorksServiceImpl();
	


	public static enum ROLE_NAME {
		LOCATEDIN_TERRITORIES, WORKS_TERRITORIES
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.LOCATEDIN_TERRITORIES, loading.Loading.EAGER);
		defaultLoadingParameters.put(ROLE_NAME.WORKS_TERRITORIES, loading.Loading.LAZY);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public TerritoriesService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public TerritoriesService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
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
	
	
	public Dataset<Territories> getTerritoriesList(){
		return getTerritoriesList(null);
	}
	
	public Dataset<Territories> getTerritoriesList(conditions.Condition<conditions.TerritoriesAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Territories>> datasets = new ArrayList<Dataset<Territories>>();
		Dataset<Territories> d = null;
		d = getTerritoriesListInTerritoriesFromMyRelDB(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsTerritories(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Territories>) r -> condition == null || condition.evaluate(r));
		d = d.dropDuplicates(new String[] {"territoryID"});
		return d;
	}
	
	
	
	
	
	public abstract Dataset<Territories> getTerritoriesListInTerritoriesFromMyRelDB(conditions.Condition<conditions.TerritoriesAttribute> condition, MutableBoolean refilterFlag);
	
	
	public Territories getTerritoriesById(String territoryID){
		Condition cond;
		cond = Condition.simple(TerritoriesAttribute.territoryID, conditions.Operator.EQUALS, territoryID);
		Dataset<Territories> res = getTerritoriesList(cond);
		if(res!=null && !res.isEmpty())
			return res.first();
		return null;
	}
	
	public Dataset<Territories> getTerritoriesListByTerritoryID(String territoryID) {
		return getTerritoriesList(conditions.Condition.simple(conditions.TerritoriesAttribute.territoryID, conditions.Operator.EQUALS, territoryID));
	}
	
	public Dataset<Territories> getTerritoriesListByTerritoryDescription(String territoryDescription) {
		return getTerritoriesList(conditions.Condition.simple(conditions.TerritoriesAttribute.territoryDescription, conditions.Operator.EQUALS, territoryDescription));
	}
	
	
	
	public static Dataset<Territories> fullOuterJoinsTerritories(List<Dataset<Territories>> datasetsPOJO) {
		return fullOuterJoinsTerritories(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Territories> fullLeftOuterJoinsTerritories(List<Dataset<Territories>> datasetsPOJO) {
		return fullOuterJoinsTerritories(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Territories> fullOuterJoinsTerritories(List<Dataset<Territories>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Territories> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("territoryID");
			logger.debug("Start {} of [{}] datasets of [Territories] objects",joinMode,datasetsPOJO.size());
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("territoryDescription", "territoryDescription_1")
								.withColumnRenamed("logEvents", "logEvents_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("territoryDescription", "territoryDescription_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
						, seq, joinMode);
			}
			logger.debug("End join. Start");
			logger.debug("Start transforming Row objects to [Territories] objects"); 
			d = res.map((MapFunction<Row, Territories>) r -> {
					Territories territories_res = new Territories();
					
					// attribute 'Territories.territoryID'
					String firstNotNull_territoryID = Util.getStringValue(r.getAs("territoryID"));
					territories_res.setTerritoryID(firstNotNull_territoryID);
					
					// attribute 'Territories.territoryDescription'
					String firstNotNull_territoryDescription = Util.getStringValue(r.getAs("territoryDescription"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String territoryDescription2 = Util.getStringValue(r.getAs("territoryDescription_" + i));
						if (firstNotNull_territoryDescription != null && territoryDescription2 != null && !firstNotNull_territoryDescription.equals(territoryDescription2)) {
							territories_res.addLogEvent("Data consistency problem for [Territories - id :"+territories_res.getTerritoryID()+"]: different values found for attribute 'Territories.territoryDescription': " + firstNotNull_territoryDescription + " and " + territoryDescription2 + "." );
							logger.warn("Data consistency problem for [Territories - id :"+territories_res.getTerritoryID()+"]: different values found for attribute 'Territories.territoryDescription': " + firstNotNull_territoryDescription + " and " + territoryDescription2 + "." );
						}
						if (firstNotNull_territoryDescription == null && territoryDescription2 != null) {
							firstNotNull_territoryDescription = territoryDescription2;
						}
					}
					territories_res.setTerritoryDescription(firstNotNull_territoryDescription);
	
					WrappedArray logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							territories_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							territories_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					return territories_res;
				}, Encoders.bean(Territories.class));
			return d;
	}
	
	
	public Dataset<Territories> getTerritoriesList(Territories.locatedIn role, Region region) {
		if(role != null) {
			if(role.equals(Territories.locatedIn.territories))
				return getTerritoriesListInLocatedInByRegion(region);
		}
		return null;
	}
	
	public Dataset<Territories> getTerritoriesList(Territories.locatedIn role, Condition<RegionAttribute> condition) {
		if(role != null) {
			if(role.equals(Territories.locatedIn.territories))
				return getTerritoriesListInLocatedInByRegionCondition(condition);
		}
		return null;
	}
	
	public Dataset<Territories> getTerritoriesList(Territories.locatedIn role, Condition<TerritoriesAttribute> condition1, Condition<RegionAttribute> condition2) {
		if(role != null) {
			if(role.equals(Territories.locatedIn.territories))
				return getTerritoriesListInLocatedIn(condition1, condition2);
		}
		return null;
	}
	
	
	
	public Dataset<Territories> getTerritoriesList(Territories.works role, Employees employees) {
		if(role != null) {
			if(role.equals(Territories.works.territories))
				return getTerritoriesListInWorksByEmployed(employees);
		}
		return null;
	}
	
	public Dataset<Territories> getTerritoriesList(Territories.works role, Condition<EmployeesAttribute> condition) {
		if(role != null) {
			if(role.equals(Territories.works.territories))
				return getTerritoriesListInWorksByEmployedCondition(condition);
		}
		return null;
	}
	
	public Dataset<Territories> getTerritoriesList(Territories.works role, Condition<EmployeesAttribute> condition1, Condition<TerritoriesAttribute> condition2) {
		if(role != null) {
			if(role.equals(Territories.works.territories))
				return getTerritoriesListInWorks(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	
	
	
	
	
	
	public abstract Dataset<Territories> getTerritoriesListInLocatedIn(conditions.Condition<conditions.TerritoriesAttribute> territories_condition,conditions.Condition<conditions.RegionAttribute> region_condition);
	
	public Dataset<Territories> getTerritoriesListInLocatedInByTerritoriesCondition(conditions.Condition<conditions.TerritoriesAttribute> territories_condition){
		return getTerritoriesListInLocatedIn(territories_condition, null);
	}
	public Dataset<Territories> getTerritoriesListInLocatedInByRegionCondition(conditions.Condition<conditions.RegionAttribute> region_condition){
		return getTerritoriesListInLocatedIn(null, region_condition);
	}
	
	public Dataset<Territories> getTerritoriesListInLocatedInByRegion(pojo.Region region){
		if(region == null)
			return null;
	
		Condition c;
		c=Condition.simple(RegionAttribute.regionID,Operator.EQUALS, region.getRegionID());
		Dataset<Territories> res = getTerritoriesListInLocatedInByRegionCondition(c);
		return res;
	}
	
	public abstract Dataset<Territories> getTerritoriesListInWorks(conditions.Condition<conditions.EmployeesAttribute> employed_condition,conditions.Condition<conditions.TerritoriesAttribute> territories_condition);
	
	public Dataset<Territories> getTerritoriesListInWorksByEmployedCondition(conditions.Condition<conditions.EmployeesAttribute> employed_condition){
		return getTerritoriesListInWorks(employed_condition, null);
	}
	
	public Dataset<Territories> getTerritoriesListInWorksByEmployed(pojo.Employees employed){
		if(employed == null)
			return null;
	
		Condition c;
		c=Condition.simple(EmployeesAttribute.employeeID,Operator.EQUALS, employed.getEmployeeID());
		Dataset<Territories> res = getTerritoriesListInWorksByEmployedCondition(c);
		return res;
	}
	
	public Dataset<Territories> getTerritoriesListInWorksByTerritoriesCondition(conditions.Condition<conditions.TerritoriesAttribute> territories_condition){
		return getTerritoriesListInWorks(null, territories_condition);
	}
	
	public abstract boolean insertTerritories(
		Territories territories,
		Region	regionLocatedIn);
	
	public abstract boolean insertTerritoriesInTerritoriesFromMyRelDB(Territories territories,
		Region	regionLocatedIn);
	private boolean inUpdateMethod = false;
	private List<Row> allTerritoriesIdList = null;
	public abstract void updateTerritoriesList(conditions.Condition<conditions.TerritoriesAttribute> condition, conditions.SetClause<conditions.TerritoriesAttribute> set);
	
	public void updateTerritories(pojo.Territories territories) {
		//TODO using the id
		return;
	}
	public abstract void updateTerritoriesListInLocatedIn(
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition,
		conditions.Condition<conditions.RegionAttribute> region_condition,
		
		conditions.SetClause<conditions.TerritoriesAttribute> set
	);
	
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
	
	public abstract void updateTerritoriesListInWorks(
		conditions.Condition<conditions.EmployeesAttribute> employed_condition,
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition,
		
		conditions.SetClause<conditions.TerritoriesAttribute> set
	);
	
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
	
	
	public abstract void deleteTerritoriesList(conditions.Condition<conditions.TerritoriesAttribute> condition);
	
	public void deleteTerritories(pojo.Territories territories) {
		//TODO using the id
		return;
	}
	public abstract void deleteTerritoriesListInLocatedIn(	
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition,	
		conditions.Condition<conditions.RegionAttribute> region_condition);
	
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
	
	public abstract void deleteTerritoriesListInWorks(	
		conditions.Condition<conditions.EmployeesAttribute> employed_condition,	
		conditions.Condition<conditions.TerritoriesAttribute> territories_condition);
	
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
