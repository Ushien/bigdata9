package tdo;

import pojo.Territories;
import java.util.List;
import java.util.ArrayList;

public class TerritoriesTDO extends Territories {
	private  String relDB_Territories_located_RegionRef; 
	public  String getRelDB_Territories_located_RegionRef() {
		return this.relDB_Territories_located_RegionRef;
	}

	public void setRelDB_Territories_located_RegionRef(  String relDB_Territories_located_RegionRef) {
		this.relDB_Territories_located_RegionRef = relDB_Territories_located_RegionRef;
	}

	private  String relDB_EmployeeTerritories_territory_TerritoryID; 
	public  String getRelDB_EmployeeTerritories_territory_TerritoryID() {
		return this.relDB_EmployeeTerritories_territory_TerritoryID;
	}

	public void setRelDB_EmployeeTerritories_territory_TerritoryID(  String relDB_EmployeeTerritories_territory_TerritoryID) {
		this.relDB_EmployeeTerritories_territory_TerritoryID = relDB_EmployeeTerritories_territory_TerritoryID;
	}

}
