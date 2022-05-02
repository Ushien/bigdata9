package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Shippers extends LoggingPojo {

	private Integer shipperID;
	private String CompanyName;
	private String Phone;

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
	public Shippers(Integer shipperID,String CompanyName,String Phone) {
		this.shipperID = shipperID;
		this.CompanyName = CompanyName;
		this.Phone = Phone;
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
		boolean eqSimpleAttr = Objects.equals(shipperID,Shippers.shipperID) && Objects.equals(CompanyName,Shippers.CompanyName) && Objects.equals(Phone,Shippers.Phone);
		boolean eqComplexAttr = false;
		eqComplexAttr = true && 
	Objects.equals(shippedOrderList, Shippers.shippedOrderList) &&
 true;
		return eqSimpleAttr && eqComplexAttr;
	}
	
	@Override
	public String toString(){
		return "Shippers { " + "shipperID="+shipperID +", "+
					"CompanyName="+CompanyName +", "+
					"Phone="+Phone +"}"; 
	}
	
	public Integer getShipperID() {
		return shipperID;
	}

	public void setShipperID(Integer shipperID) {
		this.shipperID = shipperID;
	}
	public String getCompanyName() {
		return CompanyName;
	}

	public void setCompanyName(String CompanyName) {
		this.CompanyName = CompanyName;
	}
	public String getPhone() {
		return Phone;
	}

	public void setPhone(String Phone) {
		this.Phone = Phone;
	}

	

	public List<Orders> _getShippedOrderList() {
		return shippedOrderList;
	}

	public void _setShippedOrderList(List<Orders> shippedOrderList) {
		this.shippedOrderList = shippedOrderList;
	}
}
