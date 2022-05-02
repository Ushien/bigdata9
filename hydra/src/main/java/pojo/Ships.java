package pojo;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class Ships extends LoggingPojo {

	private Orders shippedOrder;	
	private Shippers shipper;	

	//Empty constructor
	public Ships() {}
	
	//Role constructor
	public Ships(Orders shippedOrder,Shippers shipper){
		this.shippedOrder=shippedOrder;
		this.shipper=shipper;
	}
	

	@Override
    public Object clone() throws CloneNotSupportedException {
        Ships cloned = (Ships) super.clone();
		cloned.setShippedOrder((Orders)cloned.getShippedOrder().clone());	
		cloned.setShipper((Shippers)cloned.getShipper().clone());	
		return cloned;
    }

@Override
	public String toString(){
		return "ships : "+
				"roles : {" +  "shippedOrder:Orders={"+shippedOrder+"},"+ 
					 "shipper:Shippers={"+shipper+"}"+
				 "}"+
				"";
	}
	public Orders getShippedOrder() {
		return shippedOrder;
	}	

	public void setShippedOrder(Orders shippedOrder) {
		this.shippedOrder = shippedOrder;
	}
	
	public Shippers getShipper() {
		return shipper;
	}	

	public void setShipper(Shippers shipper) {
		this.shipper = shipper;
	}
	

}
