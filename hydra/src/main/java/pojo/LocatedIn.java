package pojo;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class LocatedIn extends LoggingPojo {

	private Territories territories;	
	private Region region;	

	//Empty constructor
	public LocatedIn() {}
	
	//Role constructor
	public LocatedIn(Territories territories,Region region){
		this.territories=territories;
		this.region=region;
	}
	

	@Override
    public Object clone() throws CloneNotSupportedException {
        LocatedIn cloned = (LocatedIn) super.clone();
		cloned.setTerritories((Territories)cloned.getTerritories().clone());	
		cloned.setRegion((Region)cloned.getRegion().clone());	
		return cloned;
    }

@Override
	public String toString(){
		return "locatedIn : "+
				"roles : {" +  "territories:Territories={"+territories+"},"+ 
					 "region:Region={"+region+"}"+
				 "}"+
				"";
	}
	public Territories getTerritories() {
		return territories;
	}	

	public void setTerritories(Territories territories) {
		this.territories = territories;
	}
	
	public Region getRegion() {
		return region;
	}	

	public void setRegion(Region region) {
		this.region = region;
	}
	

}
