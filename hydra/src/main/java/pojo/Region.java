package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Region extends LoggingPojo {

	private Integer regionID;
	private String RegionDescription;

	public enum locatedIn {
		region
	}
	private List<Territories> territoriesList;

	// Empty constructor
	public Region() {}

	// Constructor on Identifier
	public Region(Integer regionID){
		this.regionID = regionID;
	}
	/*
	* Constructor on simple attribute 
	*/
	public Region(Integer regionID,String RegionDescription) {
		this.regionID = regionID;
		this.RegionDescription = RegionDescription;
	}
	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

	@Override
	public boolean equals(Object o){
		if(this==o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Region Region = (Region) o;
		boolean eqSimpleAttr = Objects.equals(regionID,Region.regionID) && Objects.equals(RegionDescription,Region.RegionDescription);
		boolean eqComplexAttr = false;
		eqComplexAttr = true && 
	Objects.equals(territoriesList, Region.territoriesList) &&
 true;
		return eqSimpleAttr && eqComplexAttr;
	}
	
	@Override
	public String toString(){
		return "Region { " + "regionID="+regionID +", "+
					"RegionDescription="+RegionDescription +"}"; 
	}
	
	public Integer getRegionID() {
		return regionID;
	}

	public void setRegionID(Integer regionID) {
		this.regionID = regionID;
	}
	public String getRegionDescription() {
		return RegionDescription;
	}

	public void setRegionDescription(String RegionDescription) {
		this.RegionDescription = RegionDescription;
	}

	

	public List<Territories> _getTerritoriesList() {
		return territoriesList;
	}

	public void _setTerritoriesList(List<Territories> territoriesList) {
		this.territoriesList = territoriesList;
	}
}
