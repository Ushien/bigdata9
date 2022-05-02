package pojo;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class Buy extends LoggingPojo {

	private Orders boughtOrder;	
	private Customers customer;	

	//Empty constructor
	public Buy() {}
	
	//Role constructor
	public Buy(Orders boughtOrder,Customers customer){
		this.boughtOrder=boughtOrder;
		this.customer=customer;
	}
	

	@Override
    public Object clone() throws CloneNotSupportedException {
        Buy cloned = (Buy) super.clone();
		cloned.setBoughtOrder((Orders)cloned.getBoughtOrder().clone());	
		cloned.setCustomer((Customers)cloned.getCustomer().clone());	
		return cloned;
    }

@Override
	public String toString(){
		return "buy : "+
				"roles : {" +  "boughtOrder:Orders={"+boughtOrder+"},"+ 
					 "customer:Customers={"+customer+"}"+
				 "}"+
				"";
	}
	public Orders getBoughtOrder() {
		return boughtOrder;
	}	

	public void setBoughtOrder(Orders boughtOrder) {
		this.boughtOrder = boughtOrder;
	}
	
	public Customers getCustomer() {
		return customer;
	}	

	public void setCustomer(Customers customer) {
		this.customer = customer;
	}
	

}
