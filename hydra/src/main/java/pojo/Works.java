package pojo;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class Works extends LoggingPojo {

	private Employees employed;	
	private Territories territories;	

	//Empty constructor
	public Works() {}
	
	//Role constructor
	public Works(Employees employed,Territories territories){
		this.employed=employed;
		this.territories=territories;
	}
	

	@Override
    public Object clone() throws CloneNotSupportedException {
        Works cloned = (Works) super.clone();
		cloned.setEmployed((Employees)cloned.getEmployed().clone());	
		cloned.setTerritories((Territories)cloned.getTerritories().clone());	
		return cloned;
    }

@Override
	public String toString(){
		return "works : "+
				"roles : {" +  "employed:Employees={"+employed+"},"+ 
					 "territories:Territories={"+territories+"}"+
				 "}"+
				"";
	}
	public Employees getEmployed() {
		return employed;
	}	

	public void setEmployed(Employees employed) {
		this.employed = employed;
	}
	
	public Territories getTerritories() {
		return territories;
	}	

	public void setTerritories(Territories territories) {
		this.territories = territories;
	}
	

}
