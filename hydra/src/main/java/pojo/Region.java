package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Region extends LoggingPojo {

	private Integer regionID;
	private String regionDescription;

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
	public Region(Integer regionID,String regionDescription) {
		this.regionID = regionID;
		this.regionDescription = regionDescription;
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
		boolean eqSimpleAttr = Objects.equals(regionID,Region.regionID) && Objects.equals(regionDescription,Region.regionDescription);
		boolean eqComplexAttr = false;
		eqComplexAttr = true && 
	Objects.equals(territoriesList, Region.territoriesList) &&
 true;
		return eqSimpleAttr && eqComplexAttr;
	}
	
	@Override
	public String toString(){
		return "Region { " + "regionID="+regionID +", "+
					"regionDescription="+regionDescription +"}"; 
	}
	
	public Integer getRegionID() {
		return regionID;
	}

	public void setRegionID(Integer regionID) {
		this.regionID = regionID;
	}
	public String getRegionDescription() {
		return regionDescription;
	}

	public void setRegionDescription(String regionDescription) {
		this.regionDescription = regionDescription;
	}

	

	public List<Territories> _getTerritoriesList() {
		return territoriesList;
	}

	public void _setTerritoriesList(List<Territories> territoriesList) {
		this.territoriesList = territoriesList;
	}
}
