package tdo;

import pojo.Region;
import java.util.List;
import java.util.ArrayList;

public class RegionTDO extends Region {
	private  String relDB_Territories_located_RegionID;
	public  String getRelDB_Territories_located_RegionID() {
		return this.relDB_Territories_located_RegionID;
	}

	public void setRelDB_Territories_located_RegionID(  String relDB_Territories_located_RegionID) {
		this.relDB_Territories_located_RegionID = relDB_Territories_located_RegionID;
	}

}
