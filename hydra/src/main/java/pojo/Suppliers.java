package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Suppliers extends LoggingPojo {

	private Integer supplierId;
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
	private String HomePage;

	public enum supply {
		supplier
	}
	private List<Products> suppliedProductList;

	// Empty constructor
	public Suppliers() {}

	// Constructor on Identifier
	public Suppliers(Integer supplierId){
		this.supplierId = supplierId;
	}
	/*
	* Constructor on simple attribute 
	*/
	public Suppliers(Integer supplierId,String CompanyName,String ContactName,String ContactTitle,String Address,String City,String Region,String PostalCode,String Country,String Phone,String Fax,String HomePage) {
		this.supplierId = supplierId;
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
		this.HomePage = HomePage;
	}
	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

	@Override
	public boolean equals(Object o){
		if(this==o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Suppliers Suppliers = (Suppliers) o;
		boolean eqSimpleAttr = Objects.equals(supplierId,Suppliers.supplierId) && Objects.equals(CompanyName,Suppliers.CompanyName) && Objects.equals(ContactName,Suppliers.ContactName) && Objects.equals(ContactTitle,Suppliers.ContactTitle) && Objects.equals(Address,Suppliers.Address) && Objects.equals(City,Suppliers.City) && Objects.equals(Region,Suppliers.Region) && Objects.equals(PostalCode,Suppliers.PostalCode) && Objects.equals(Country,Suppliers.Country) && Objects.equals(Phone,Suppliers.Phone) && Objects.equals(Fax,Suppliers.Fax) && Objects.equals(HomePage,Suppliers.HomePage);
		boolean eqComplexAttr = false;
		eqComplexAttr = true && 
	Objects.equals(suppliedProductList, Suppliers.suppliedProductList) &&
 true;
		return eqSimpleAttr && eqComplexAttr;
	}
	
	@Override
	public String toString(){
		return "Suppliers { " + "supplierId="+supplierId +", "+
					"CompanyName="+CompanyName +", "+
					"ContactName="+ContactName +", "+
					"ContactTitle="+ContactTitle +", "+
					"Address="+Address +", "+
					"City="+City +", "+
					"Region="+Region +", "+
					"PostalCode="+PostalCode +", "+
					"Country="+Country +", "+
					"Phone="+Phone +", "+
					"Fax="+Fax +", "+
					"HomePage="+HomePage +"}"; 
	}
	
	public Integer getSupplierId() {
		return supplierId;
	}

	public void setSupplierId(Integer supplierId) {
		this.supplierId = supplierId;
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
	public String getHomePage() {
		return HomePage;
	}

	public void setHomePage(String HomePage) {
		this.HomePage = HomePage;
	}

	

	public List<Products> _getSuppliedProductList() {
		return suppliedProductList;
	}

	public void _setSuppliedProductList(List<Products> suppliedProductList) {
		this.suppliedProductList = suppliedProductList;
	}
}
