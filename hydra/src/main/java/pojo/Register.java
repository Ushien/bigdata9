package pojo;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class Register extends LoggingPojo {

	private Orders processedOrder;	
	private Employees employeeInCharge;	

	//Empty constructor
	public Register() {}
	
	//Role constructor
	public Register(Orders processedOrder,Employees employeeInCharge){
		this.processedOrder=processedOrder;
		this.employeeInCharge=employeeInCharge;
	}
	

	@Override
    public Object clone() throws CloneNotSupportedException {
        Register cloned = (Register) super.clone();
		cloned.setProcessedOrder((Orders)cloned.getProcessedOrder().clone());	
		cloned.setEmployeeInCharge((Employees)cloned.getEmployeeInCharge().clone());	
		return cloned;
    }

@Override
	public String toString(){
		return "register : "+
				"roles : {" +  "processedOrder:Orders={"+processedOrder+"},"+ 
					 "employeeInCharge:Employees={"+employeeInCharge+"}"+
				 "}"+
				"";
	}
	public Orders getProcessedOrder() {
		return processedOrder;
	}	

	public void setProcessedOrder(Orders processedOrder) {
		this.processedOrder = processedOrder;
	}
	
	public Employees getEmployeeInCharge() {
		return employeeInCharge;
	}	

	public void setEmployeeInCharge(Employees employeeInCharge) {
		this.employeeInCharge = employeeInCharge;
	}
	

}
