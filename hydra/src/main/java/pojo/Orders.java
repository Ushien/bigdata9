package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Orders extends LoggingPojo {

	private Integer id;
	private LocalDate orderDate;
	private LocalDate requiredDate;
	private LocalDate shippedDate;
	private Double freight;
	private String shipName;
	private String shipAddress;
	private String shipCity;
	private String shipRegion;
	private String shipPostalCode;
	private String shipCountry;

	public enum buy {
		boughtOrder
	}
	private Customers customer;
	public enum register {
		processedOrder
	}
	private Employees employeeInCharge;
	public enum ships {
		shippedOrder
	}
	private Shippers shipper;
	private List<ComposedOf> composedOfListAsOrder;

	// Empty constructor
	public Orders() {}

	// Constructor on Identifier
	public Orders(Integer id){
		this.id = id;
	}
	/*
	* Constructor on simple attribute 
	*/
	public Orders(Integer id,LocalDate orderDate,LocalDate requiredDate,LocalDate shippedDate,Double freight,String shipName,String shipAddress,String shipCity,String shipRegion,String shipPostalCode,String shipCountry) {
		this.id = id;
		this.orderDate = orderDate;
		this.requiredDate = requiredDate;
		this.shippedDate = shippedDate;
		this.freight = freight;
		this.shipName = shipName;
		this.shipAddress = shipAddress;
		this.shipCity = shipCity;
		this.shipRegion = shipRegion;
		this.shipPostalCode = shipPostalCode;
		this.shipCountry = shipCountry;
	}
	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

	@Override
	public boolean equals(Object o){
		if(this==o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Orders Orders = (Orders) o;
		boolean eqSimpleAttr = Objects.equals(id,Orders.id) && Objects.equals(orderDate,Orders.orderDate) && Objects.equals(requiredDate,Orders.requiredDate) && Objects.equals(shippedDate,Orders.shippedDate) && Objects.equals(freight,Orders.freight) && Objects.equals(shipName,Orders.shipName) && Objects.equals(shipAddress,Orders.shipAddress) && Objects.equals(shipCity,Orders.shipCity) && Objects.equals(shipRegion,Orders.shipRegion) && Objects.equals(shipPostalCode,Orders.shipPostalCode) && Objects.equals(shipCountry,Orders.shipCountry);
		boolean eqComplexAttr = false;
		eqComplexAttr = true && 
	Objects.equals(customer, Orders.customer) &&
	Objects.equals(employeeInCharge, Orders.employeeInCharge) &&
	Objects.equals(shipper, Orders.shipper) &&
	Objects.equals(composedOfListAsOrder,Orders.composedOfListAsOrder) &&
 true;
		return eqSimpleAttr && eqComplexAttr;
	}
	
	@Override
	public String toString(){
		return "Orders { " + "id="+id +", "+
					"orderDate="+orderDate +", "+
					"requiredDate="+requiredDate +", "+
					"shippedDate="+shippedDate +", "+
					"freight="+freight +", "+
					"shipName="+shipName +", "+
					"shipAddress="+shipAddress +", "+
					"shipCity="+shipCity +", "+
					"shipRegion="+shipRegion +", "+
					"shipPostalCode="+shipPostalCode +", "+
					"shipCountry="+shipCountry +"}"; 
	}
	
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}
	public LocalDate getOrderDate() {
		return orderDate;
	}

	public void setOrderDate(LocalDate orderDate) {
		this.orderDate = orderDate;
	}
	public LocalDate getRequiredDate() {
		return requiredDate;
	}

	public void setRequiredDate(LocalDate requiredDate) {
		this.requiredDate = requiredDate;
	}
	public LocalDate getShippedDate() {
		return shippedDate;
	}

	public void setShippedDate(LocalDate shippedDate) {
		this.shippedDate = shippedDate;
	}
	public Double getFreight() {
		return freight;
	}

	public void setFreight(Double freight) {
		this.freight = freight;
	}
	public String getShipName() {
		return shipName;
	}

	public void setShipName(String shipName) {
		this.shipName = shipName;
	}
	public String getShipAddress() {
		return shipAddress;
	}

	public void setShipAddress(String shipAddress) {
		this.shipAddress = shipAddress;
	}
	public String getShipCity() {
		return shipCity;
	}

	public void setShipCity(String shipCity) {
		this.shipCity = shipCity;
	}
	public String getShipRegion() {
		return shipRegion;
	}

	public void setShipRegion(String shipRegion) {
		this.shipRegion = shipRegion;
	}
	public String getShipPostalCode() {
		return shipPostalCode;
	}

	public void setShipPostalCode(String shipPostalCode) {
		this.shipPostalCode = shipPostalCode;
	}
	public String getShipCountry() {
		return shipCountry;
	}

	public void setShipCountry(String shipCountry) {
		this.shipCountry = shipCountry;
	}

	

	public Customers _getCustomer() {
		return customer;
	}

	public void _setCustomer(Customers customer) {
		this.customer = customer;
	}
	public Employees _getEmployeeInCharge() {
		return employeeInCharge;
	}

	public void _setEmployeeInCharge(Employees employeeInCharge) {
		this.employeeInCharge = employeeInCharge;
	}
	public Shippers _getShipper() {
		return shipper;
	}

	public void _setShipper(Shippers shipper) {
		this.shipper = shipper;
	}
	public java.util.List<ComposedOf> _getComposedOfListAsOrder() {
		return composedOfListAsOrder;
	}

	public void _setComposedOfListAsOrder(java.util.List<ComposedOf> composedOfListAsOrder) {
		this.composedOfListAsOrder = composedOfListAsOrder;
	}
}
