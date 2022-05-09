package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Shippers extends LoggingPojo {

	private Integer shipperID;
	private String companyName;
	private String phone;

	public enum ships {
		shipper
	}
	private List<Orders> shippedOrderList;

	// Empty constructor
	public Shippers() {}

	// Constructor on Identifier
	public Shippers(Integer shipperID){
		this.shipperID = shipperID;
	}
	/*
	* Constructor on simple attribute 
	*/
	public Shippers(Integer shipperID,String companyName,String phone) {
		this.shipperID = shipperID;
		this.companyName = companyName;
		this.phone = phone;
	}
	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

	@Override
	public boolean equals(Object o){
		if(this==o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Shippers Shippers = (Shippers) o;
		boolean eqSimpleAttr = Objects.equals(shipperID,Shippers.shipperID) && Objects.equals(companyName,Shippers.companyName) && Objects.equals(phone,Shippers.phone);
		boolean eqComplexAttr = false;
		eqComplexAttr = true && 
	Objects.equals(shippedOrderList, Shippers.shippedOrderList) &&
 true;
		return eqSimpleAttr && eqComplexAttr;
	}
	
	@Override
	public String toString(){
		return "Shippers { " + "shipperID="+shipperID +", "+
					"companyName="+companyName +", "+
					"phone="+phone +"}"; 
	}
	
	public Integer getShipperID() {
		return shipperID;
	}

	public void setShipperID(Integer shipperID) {
		this.shipperID = shipperID;
	}
	public String getCompanyName() {
		return companyName;
	}

	public void setCompanyName(String companyName) {
		this.companyName = companyName;
	}
	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	

	public List<Orders> _getShippedOrderList() {
		return shippedOrderList;
	}

	public void _setShippedOrderList(List<Orders> shippedOrderList) {
		this.shippedOrderList = shippedOrderList;
	}
}
