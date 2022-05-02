package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Orders extends LoggingPojo {

	private Integer id;
	private LocalDate OrderDate;
	private LocalDate RequiredDate;
	private LocalDate ShippedDate;
	private Double Freight;
	private String ShipName;
	private String ShipAddress;
	private String ShipCity;
	private String ShipRegion;
	private String ShipPostalCode;
	private String ShipCountry;

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
	public Orders(Integer id,LocalDate OrderDate,LocalDate RequiredDate,LocalDate ShippedDate,Double Freight,String ShipName,String ShipAddress,String ShipCity,String ShipRegion,String ShipPostalCode,String ShipCountry) {
		this.id = id;
		this.OrderDate = OrderDate;
		this.RequiredDate = RequiredDate;
		this.ShippedDate = ShippedDate;
		this.Freight = Freight;
		this.ShipName = ShipName;
		this.ShipAddress = ShipAddress;
		this.ShipCity = ShipCity;
		this.ShipRegion = ShipRegion;
		this.ShipPostalCode = ShipPostalCode;
		this.ShipCountry = ShipCountry;
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
		boolean eqSimpleAttr = Objects.equals(id,Orders.id) && Objects.equals(OrderDate,Orders.OrderDate) && Objects.equals(RequiredDate,Orders.RequiredDate) && Objects.equals(ShippedDate,Orders.ShippedDate) && Objects.equals(Freight,Orders.Freight) && Objects.equals(ShipName,Orders.ShipName) && Objects.equals(ShipAddress,Orders.ShipAddress) && Objects.equals(ShipCity,Orders.ShipCity) && Objects.equals(ShipRegion,Orders.ShipRegion) && Objects.equals(ShipPostalCode,Orders.ShipPostalCode) && Objects.equals(ShipCountry,Orders.ShipCountry);
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
					"OrderDate="+OrderDate +", "+
					"RequiredDate="+RequiredDate +", "+
					"ShippedDate="+ShippedDate +", "+
					"Freight="+Freight +", "+
					"ShipName="+ShipName +", "+
					"ShipAddress="+ShipAddress +", "+
					"ShipCity="+ShipCity +", "+
					"ShipRegion="+ShipRegion +", "+
					"ShipPostalCode="+ShipPostalCode +", "+
					"ShipCountry="+ShipCountry +"}"; 
	}
	
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}
	public LocalDate getOrderDate() {
		return OrderDate;
	}

	public void setOrderDate(LocalDate OrderDate) {
		this.OrderDate = OrderDate;
	}
	public LocalDate getRequiredDate() {
		return RequiredDate;
	}

	public void setRequiredDate(LocalDate RequiredDate) {
		this.RequiredDate = RequiredDate;
	}
	public LocalDate getShippedDate() {
		return ShippedDate;
	}

	public void setShippedDate(LocalDate ShippedDate) {
		this.ShippedDate = ShippedDate;
	}
	public Double getFreight() {
		return Freight;
	}

	public void setFreight(Double Freight) {
		this.Freight = Freight;
	}
	public String getShipName() {
		return ShipName;
	}

	public void setShipName(String ShipName) {
		this.ShipName = ShipName;
	}
	public String getShipAddress() {
		return ShipAddress;
	}

	public void setShipAddress(String ShipAddress) {
		this.ShipAddress = ShipAddress;
	}
	public String getShipCity() {
		return ShipCity;
	}

	public void setShipCity(String ShipCity) {
		this.ShipCity = ShipCity;
	}
	public String getShipRegion() {
		return ShipRegion;
	}

	public void setShipRegion(String ShipRegion) {
		this.ShipRegion = ShipRegion;
	}
	public String getShipPostalCode() {
		return ShipPostalCode;
	}

	public void setShipPostalCode(String ShipPostalCode) {
		this.ShipPostalCode = ShipPostalCode;
	}
	public String getShipCountry() {
		return ShipCountry;
	}

	public void setShipCountry(String ShipCountry) {
		this.ShipCountry = ShipCountry;
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
