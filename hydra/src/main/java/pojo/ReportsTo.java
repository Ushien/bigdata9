package pojo;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class ReportsTo extends LoggingPojo {

	private Employees subordonee;	
	private Employees boss;	

	//Empty constructor
	public ReportsTo() {}
	
	//Role constructor
	public ReportsTo(Employees subordonee,Employees boss){
		this.subordonee=subordonee;
		this.boss=boss;
	}
	

	@Override
    public Object clone() throws CloneNotSupportedException {
        ReportsTo cloned = (ReportsTo) super.clone();
		cloned.setSubordonee((Employees)cloned.getSubordonee().clone());	
		cloned.setBoss((Employees)cloned.getBoss().clone());	
		return cloned;
    }

@Override
	public String toString(){
		return "reportsTo : "+
				"roles : {" +  "subordonee:Employees={"+subordonee+"},"+ 
					 "boss:Employees={"+boss+"}"+
				 "}"+
				"";
	}
	public Employees getSubordonee() {
		return subordonee;
	}	

	public void setSubordonee(Employees subordonee) {
		this.subordonee = subordonee;
	}
	
	public Employees getBoss() {
		return boss;
	}	

	public void setBoss(Employees boss) {
		this.boss = boss;
	}
	

}
