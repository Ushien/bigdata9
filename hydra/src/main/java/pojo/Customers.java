package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Customers extends LoggingPojo {

	private String customerID;
	private String companyName;
	private String contactName;
	private String contactTitle;
	private String address;
	private String city;
	private String region;
	private String postalCode;
	private String country;
	private String phone;
	private String fax;

	public enum buy {
		customer
	}
	private List<Orders> boughtOrderList;

	// Empty constructor
	public Customers() {}

	// Constructor on Identifier
	public Customers(String customerID){
		this.customerID = customerID;
	}
	/*
	* Constructor on simple attribute 
	*/
	public Customers(String customerID,String companyName,String contactName,String contactTitle,String address,String city,String region,String postalCode,String country,String phone,String fax) {
		this.customerID = customerID;
		this.companyName = companyName;
		this.contactName = contactName;
		this.contactTitle = contactTitle;
		this.address = address;
		this.city = city;
		this.region = region;
		this.postalCode = postalCode;
		this.country = country;
		this.phone = phone;
		this.fax = fax;
	}
	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

	@Override
	public boolean equals(Object o){
		if(this==o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Customers Customers = (Customers) o;
		boolean eqSimpleAttr = Objects.equals(customerID,Customers.customerID) && Objects.equals(companyName,Customers.companyName) && Objects.equals(contactName,Customers.contactName) && Objects.equals(contactTitle,Customers.contactTitle) && Objects.equals(address,Customers.address) && Objects.equals(city,Customers.city) && Objects.equals(region,Customers.region) && Objects.equals(postalCode,Customers.postalCode) && Objects.equals(country,Customers.country) && Objects.equals(phone,Customers.phone) && Objects.equals(fax,Customers.fax);
		boolean eqComplexAttr = false;
		eqComplexAttr = true && 
	Objects.equals(boughtOrderList, Customers.boughtOrderList) &&
 true;
		return eqSimpleAttr && eqComplexAttr;
	}
	
	@Override
	public String toString(){
		return "Customers { " + "customerID="+customerID +", "+
					"companyName="+companyName +", "+
					"contactName="+contactName +", "+
					"contactTitle="+contactTitle +", "+
					"address="+address +", "+
					"city="+city +", "+
					"region="+region +", "+
					"postalCode="+postalCode +", "+
					"country="+country +", "+
					"phone="+phone +", "+
					"fax="+fax +"}"; 
	}
	
	public String getCustomerID() {
		return customerID;
	}

	public void setCustomerID(String customerID) {
		this.customerID = customerID;
	}
	public String getCompanyName() {
		return companyName;
	}

	public void setCompanyName(String companyName) {
		this.companyName = companyName;
	}
	public String getContactName() {
		return contactName;
	}

	public void setContactName(String contactName) {
		this.contactName = contactName;
	}
	public String getContactTitle() {
		return contactTitle;
	}

	public void setContactTitle(String contactTitle) {
		this.contactTitle = contactTitle;
	}
	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}
	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}
	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}
	public String getPostalCode() {
		return postalCode;
	}

	public void setPostalCode(String postalCode) {
		this.postalCode = postalCode;
	}
	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}
	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}
	public String getFax() {
		return fax;
	}

	public void setFax(String fax) {
		this.fax = fax;
	}

	

	public List<Orders> _getBoughtOrderList() {
		return boughtOrderList;
	}

	public void _setBoughtOrderList(List<Orders> boughtOrderList) {
		this.boughtOrderList = boughtOrderList;
	}
}
