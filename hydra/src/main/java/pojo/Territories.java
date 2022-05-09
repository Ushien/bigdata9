package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Territories extends LoggingPojo {

	private String territoryID;
	private String territoryDescription;

	public enum locatedIn {
		territories
	}
	private Region region;
	public enum works {
		territories
	}
	private List<Employees> employedList;

	// Empty constructor
	public Territories() {}

	// Constructor on Identifier
	public Territories(String territoryID){
		this.territoryID = territoryID;
	}
	/*
	* Constructor on simple attribute 
	*/
	public Territories(String territoryID,String territoryDescription) {
		this.territoryID = territoryID;
		this.territoryDescription = territoryDescription;
	}
	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

	@Override
	public boolean equals(Object o){
		if(this==o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Territories Territories = (Territories) o;
		boolean eqSimpleAttr = Objects.equals(territoryID,Territories.territoryID) && Objects.equals(territoryDescription,Territories.territoryDescription);
		boolean eqComplexAttr = false;
		eqComplexAttr = true && 
	Objects.equals(region, Territories.region) &&
	Objects.equals(employedList, Territories.employedList) &&
 true;
		return eqSimpleAttr && eqComplexAttr;
	}
	
	@Override
	public String toString(){
		return "Territories { " + "territoryID="+territoryID +", "+
					"territoryDescription="+territoryDescription +"}"; 
	}
	
	public String getTerritoryID() {
		return territoryID;
	}

	public void setTerritoryID(String territoryID) {
		this.territoryID = territoryID;
	}
	public String getTerritoryDescription() {
		return territoryDescription;
	}

	public void setTerritoryDescription(String territoryDescription) {
		this.territoryDescription = territoryDescription;
	}

	

	public Region _getRegion() {
		return region;
	}

	public void _setRegion(Region region) {
		this.region = region;
	}
	public List<Employees> _getEmployedList() {
		return employedList;
	}

	public void _setEmployedList(List<Employees> employedList) {
		this.employedList = employedList;
	}
}
