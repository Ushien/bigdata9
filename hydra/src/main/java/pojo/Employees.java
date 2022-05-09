package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Employees extends LoggingPojo {

	private Integer employeeID;
	private String lastName;
	private String firstName;
	private String title;
	private String titleOfCourtesy;
	private LocalDate birthDate;
	private LocalDate hireDate;
	private String address;
	private String city;
	private String region;
	private String postalCode;
	private String country;
	private String homePhone;
	private String extension;
	private byte[] photo;
	private String notes;
	private String photoPath;
	private Double salary;

	public enum works {
		employed
	}
	private List<Territories> territoriesList;
	public enum reportsTo {
		subordonee, boss
	}
	private Employees boss;
	private List<Employees> subordoneeList;
	public enum register {
		employeeInCharge
	}
	private List<Orders> processedOrderList;

	// Empty constructor
	public Employees() {}

	// Constructor on Identifier
	public Employees(Integer employeeID){
		this.employeeID = employeeID;
	}
	/*
	* Constructor on simple attribute 
	*/
	public Employees(Integer employeeID,String lastName,String firstName,String title,String titleOfCourtesy,LocalDate birthDate,LocalDate hireDate,String address,String city,String region,String postalCode,String country,String homePhone,String extension,byte[] photo,String notes,String photoPath,Double salary) {
		this.employeeID = employeeID;
		this.lastName = lastName;
		this.firstName = firstName;
		this.title = title;
		this.titleOfCourtesy = titleOfCourtesy;
		this.birthDate = birthDate;
		this.hireDate = hireDate;
		this.address = address;
		this.city = city;
		this.region = region;
		this.postalCode = postalCode;
		this.country = country;
		this.homePhone = homePhone;
		this.extension = extension;
		this.photo = photo;
		this.notes = notes;
		this.photoPath = photoPath;
		this.salary = salary;
	}
	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

	@Override
	public boolean equals(Object o){
		if(this==o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Employees Employees = (Employees) o;
		boolean eqSimpleAttr = Objects.equals(employeeID,Employees.employeeID) && Objects.equals(lastName,Employees.lastName) && Objects.equals(firstName,Employees.firstName) && Objects.equals(title,Employees.title) && Objects.equals(titleOfCourtesy,Employees.titleOfCourtesy) && Objects.equals(birthDate,Employees.birthDate) && Objects.equals(hireDate,Employees.hireDate) && Objects.equals(address,Employees.address) && Objects.equals(city,Employees.city) && Objects.equals(region,Employees.region) && Objects.equals(postalCode,Employees.postalCode) && Objects.equals(country,Employees.country) && Objects.equals(homePhone,Employees.homePhone) && Objects.equals(extension,Employees.extension) && Objects.equals(photo,Employees.photo) && Objects.equals(notes,Employees.notes) && Objects.equals(photoPath,Employees.photoPath) && Objects.equals(salary,Employees.salary);
		boolean eqComplexAttr = false;
		eqComplexAttr = true && 
	Objects.equals(territoriesList, Employees.territoriesList) &&
	Objects.equals(boss, Employees.boss) &&
	Objects.equals(subordoneeList, Employees.subordoneeList) &&
	Objects.equals(processedOrderList, Employees.processedOrderList) &&
 true;
		return eqSimpleAttr && eqComplexAttr;
	}
	
	@Override
	public String toString(){
		return "Employees { " + "employeeID="+employeeID +", "+
					"lastName="+lastName +", "+
					"firstName="+firstName +", "+
					"title="+title +", "+
					"titleOfCourtesy="+titleOfCourtesy +", "+
					"birthDate="+birthDate +", "+
					"hireDate="+hireDate +", "+
					"address="+address +", "+
					"city="+city +", "+
					"region="+region +", "+
					"postalCode="+postalCode +", "+
					"country="+country +", "+
					"homePhone="+homePhone +", "+
					"extension="+extension +", "+
					"photo="+photo +", "+
					"notes="+notes +", "+
					"photoPath="+photoPath +", "+
					"salary="+salary +"}"; 
	}
	
	public Integer getEmployeeID() {
		return employeeID;
	}

	public void setEmployeeID(Integer employeeID) {
		this.employeeID = employeeID;
	}
	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}
	public String getTitleOfCourtesy() {
		return titleOfCourtesy;
	}

	public void setTitleOfCourtesy(String titleOfCourtesy) {
		this.titleOfCourtesy = titleOfCourtesy;
	}
	public LocalDate getBirthDate() {
		return birthDate;
	}

	public void setBirthDate(LocalDate birthDate) {
		this.birthDate = birthDate;
	}
	public LocalDate getHireDate() {
		return hireDate;
	}

	public void setHireDate(LocalDate hireDate) {
		this.hireDate = hireDate;
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
	public String getHomePhone() {
		return homePhone;
	}

	public void setHomePhone(String homePhone) {
		this.homePhone = homePhone;
	}
	public String getExtension() {
		return extension;
	}

	public void setExtension(String extension) {
		this.extension = extension;
	}
	public byte[] getPhoto() {
		return photo;
	}

	public void setPhoto(byte[] photo) {
		this.photo = photo;
	}
	public String getNotes() {
		return notes;
	}

	public void setNotes(String notes) {
		this.notes = notes;
	}
	public String getPhotoPath() {
		return photoPath;
	}

	public void setPhotoPath(String photoPath) {
		this.photoPath = photoPath;
	}
	public Double getSalary() {
		return salary;
	}

	public void setSalary(Double salary) {
		this.salary = salary;
	}

	

	public List<Territories> _getTerritoriesList() {
		return territoriesList;
	}

	public void _setTerritoriesList(List<Territories> territoriesList) {
		this.territoriesList = territoriesList;
	}
	public Employees _getBoss() {
		return boss;
	}

	public void _setBoss(Employees boss) {
		this.boss = boss;
	}
	public List<Employees> _getSubordoneeList() {
		return subordoneeList;
	}

	public void _setSubordoneeList(List<Employees> subordoneeList) {
		this.subordoneeList = subordoneeList;
	}
	public List<Orders> _getProcessedOrderList() {
		return processedOrderList;
	}

	public void _setProcessedOrderList(List<Orders> processedOrderList) {
		this.processedOrderList = processedOrderList;
	}
}
