package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Suppliers extends LoggingPojo {

	private Integer supplierId;
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
	private String homePage;

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
	public Suppliers(Integer supplierId,String companyName,String contactName,String contactTitle,String address,String city,String region,String postalCode,String country,String phone,String fax,String homePage) {
		this.supplierId = supplierId;
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
		this.homePage = homePage;
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
		boolean eqSimpleAttr = Objects.equals(supplierId,Suppliers.supplierId) && Objects.equals(companyName,Suppliers.companyName) && Objects.equals(contactName,Suppliers.contactName) && Objects.equals(contactTitle,Suppliers.contactTitle) && Objects.equals(address,Suppliers.address) && Objects.equals(city,Suppliers.city) && Objects.equals(region,Suppliers.region) && Objects.equals(postalCode,Suppliers.postalCode) && Objects.equals(country,Suppliers.country) && Objects.equals(phone,Suppliers.phone) && Objects.equals(fax,Suppliers.fax) && Objects.equals(homePage,Suppliers.homePage);
		boolean eqComplexAttr = false;
		eqComplexAttr = true && 
	Objects.equals(suppliedProductList, Suppliers.suppliedProductList) &&
 true;
		return eqSimpleAttr && eqComplexAttr;
	}
	
	@Override
	public String toString(){
		return "Suppliers { " + "supplierId="+supplierId +", "+
					"companyName="+companyName +", "+
					"contactName="+contactName +", "+
					"contactTitle="+contactTitle +", "+
					"address="+address +", "+
					"city="+city +", "+
					"region="+region +", "+
					"postalCode="+postalCode +", "+
					"country="+country +", "+
					"phone="+phone +", "+
					"fax="+fax +", "+
					"homePage="+homePage +"}"; 
	}
	
	public Integer getSupplierId() {
		return supplierId;
	}

	public void setSupplierId(Integer supplierId) {
		this.supplierId = supplierId;
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
	public String getHomePage() {
		return homePage;
	}

	public void setHomePage(String homePage) {
		this.homePage = homePage;
	}

	

	public List<Products> _getSuppliedProductList() {
		return suppliedProductList;
	}

	public void _setSuppliedProductList(List<Products> suppliedProductList) {
		this.suppliedProductList = suppliedProductList;
	}
}
