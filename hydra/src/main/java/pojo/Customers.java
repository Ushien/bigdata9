package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Customers extends LoggingPojo {

	private String customerID;
	private String CompanyName;
	private String ContactName;
	private String ContactTitle;
	private String Address;
	private String City;
	private String Region;
	private String PostalCode;
	private String Country;
	private String Phone;
	private String Fax;

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
	public Customers(String customerID,String CompanyName,String ContactName,String ContactTitle,String Address,String City,String Region,String PostalCode,String Country,String Phone,String Fax) {
		this.customerID = customerID;
		this.CompanyName = CompanyName;
		this.ContactName = ContactName;
		this.ContactTitle = ContactTitle;
		this.Address = Address;
		this.City = City;
		this.Region = Region;
		this.PostalCode = PostalCode;
		this.Country = Country;
		this.Phone = Phone;
		this.Fax = Fax;
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
		boolean eqSimpleAttr = Objects.equals(customerID,Customers.customerID) && Objects.equals(CompanyName,Customers.CompanyName) && Objects.equals(ContactName,Customers.ContactName) && Objects.equals(ContactTitle,Customers.ContactTitle) && Objects.equals(Address,Customers.Address) && Objects.equals(City,Customers.City) && Objects.equals(Region,Customers.Region) && Objects.equals(PostalCode,Customers.PostalCode) && Objects.equals(Country,Customers.Country) && Objects.equals(Phone,Customers.Phone) && Objects.equals(Fax,Customers.Fax);
		boolean eqComplexAttr = false;
		eqComplexAttr = true && 
	Objects.equals(boughtOrderList, Customers.boughtOrderList) &&
 true;
		return eqSimpleAttr && eqComplexAttr;
	}
	
	@Override
	public String toString(){
		return "Customers { " + "customerID="+customerID +", "+
					"CompanyName="+CompanyName +", "+
					"ContactName="+ContactName +", "+
					"ContactTitle="+ContactTitle +", "+
					"Address="+Address +", "+
					"City="+City +", "+
					"Region="+Region +", "+
					"PostalCode="+PostalCode +", "+
					"Country="+Country +", "+
					"Phone="+Phone +", "+
					"Fax="+Fax +"}"; 
	}
	
	public String getCustomerID() {
		return customerID;
	}

	public void setCustomerID(String customerID) {
		this.customerID = customerID;
	}
	public String getCompanyName() {
		return CompanyName;
	}

	public void setCompanyName(String CompanyName) {
		this.CompanyName = CompanyName;
	}
	public String getContactName() {
		return ContactName;
	}

	public void setContactName(String ContactName) {
		this.ContactName = ContactName;
	}
	public String getContactTitle() {
		return ContactTitle;
	}

	public void setContactTitle(String ContactTitle) {
		this.ContactTitle = ContactTitle;
	}
	public String getAddress() {
		return Address;
	}

	public void setAddress(String Address) {
		this.Address = Address;
	}
	public String getCity() {
		return City;
	}

	public void setCity(String City) {
		this.City = City;
	}
	public String getRegion() {
		return Region;
	}

	public void setRegion(String Region) {
		this.Region = Region;
	}
	public String getPostalCode() {
		return PostalCode;
	}

	public void setPostalCode(String PostalCode) {
		this.PostalCode = PostalCode;
	}
	public String getCountry() {
		return Country;
	}

	public void setCountry(String Country) {
		this.Country = Country;
	}
	public String getPhone() {
		return Phone;
	}

	public void setPhone(String Phone) {
		this.Phone = Phone;
	}
	public String getFax() {
		return Fax;
	}

	public void setFax(String Fax) {
		this.Fax = Fax;
	}

	

	public List<Orders> _getBoughtOrderList() {
		return boughtOrderList;
	}

	public void _setBoughtOrderList(List<Orders> boughtOrderList) {
		this.boughtOrderList = boughtOrderList;
	}
}
