package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Employees extends LoggingPojo {

	private Integer employeeID;
	private String LastName;
	private String FirstName;
	private String Title;
	private String TitleOfCourtesy;
	private LocalDate BirthDate;
	private LocalDate HireDate;
	private String Address;
	private String City;
	private String Region;
	private String PostalCode;
	private String Country;
	private String HomePhone;
	private String Extension;
	private byte[] Photo;
	private String Notes;
	private String PhotoPath;
	private Double Salary;

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
	public Employees(Integer employeeID,String LastName,String FirstName,String Title,String TitleOfCourtesy,LocalDate BirthDate,LocalDate HireDate,String Address,String City,String Region,String PostalCode,String Country,String HomePhone,String Extension,byte[] Photo,String Notes,String PhotoPath,Double Salary) {
		this.employeeID = employeeID;
		this.LastName = LastName;
		this.FirstName = FirstName;
		this.Title = Title;
		this.TitleOfCourtesy = TitleOfCourtesy;
		this.BirthDate = BirthDate;
		this.HireDate = HireDate;
		this.Address = Address;
		this.City = City;
		this.Region = Region;
		this.PostalCode = PostalCode;
		this.Country = Country;
		this.HomePhone = HomePhone;
		this.Extension = Extension;
		this.Photo = Photo;
		this.Notes = Notes;
		this.PhotoPath = PhotoPath;
		this.Salary = Salary;
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
		boolean eqSimpleAttr = Objects.equals(employeeID,Employees.employeeID) && Objects.equals(LastName,Employees.LastName) && Objects.equals(FirstName,Employees.FirstName) && Objects.equals(Title,Employees.Title) && Objects.equals(TitleOfCourtesy,Employees.TitleOfCourtesy) && Objects.equals(BirthDate,Employees.BirthDate) && Objects.equals(HireDate,Employees.HireDate) && Objects.equals(Address,Employees.Address) && Objects.equals(City,Employees.City) && Objects.equals(Region,Employees.Region) && Objects.equals(PostalCode,Employees.PostalCode) && Objects.equals(Country,Employees.Country) && Objects.equals(HomePhone,Employees.HomePhone) && Objects.equals(Extension,Employees.Extension) && Objects.equals(Photo,Employees.Photo) && Objects.equals(Notes,Employees.Notes) && Objects.equals(PhotoPath,Employees.PhotoPath) && Objects.equals(Salary,Employees.Salary);
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
					"LastName="+LastName +", "+
					"FirstName="+FirstName +", "+
					"Title="+Title +", "+
					"TitleOfCourtesy="+TitleOfCourtesy +", "+
					"BirthDate="+BirthDate +", "+
					"HireDate="+HireDate +", "+
					"Address="+Address +", "+
					"City="+City +", "+
					"Region="+Region +", "+
					"PostalCode="+PostalCode +", "+
					"Country="+Country +", "+
					"HomePhone="+HomePhone +", "+
					"Extension="+Extension +", "+
					"Photo="+Photo +", "+
					"Notes="+Notes +", "+
					"PhotoPath="+PhotoPath +", "+
					"Salary="+Salary +"}"; 
	}
	
	public Integer getEmployeeID() {
		return employeeID;
	}

	public void setEmployeeID(Integer employeeID) {
		this.employeeID = employeeID;
	}
	public String getLastName() {
		return LastName;
	}

	public void setLastName(String LastName) {
		this.LastName = LastName;
	}
	public String getFirstName() {
		return FirstName;
	}

	public void setFirstName(String FirstName) {
		this.FirstName = FirstName;
	}
	public String getTitle() {
		return Title;
	}

	public void setTitle(String Title) {
		this.Title = Title;
	}
	public String getTitleOfCourtesy() {
		return TitleOfCourtesy;
	}

	public void setTitleOfCourtesy(String TitleOfCourtesy) {
		this.TitleOfCourtesy = TitleOfCourtesy;
	}
	public LocalDate getBirthDate() {
		return BirthDate;
	}

	public void setBirthDate(LocalDate BirthDate) {
		this.BirthDate = BirthDate;
	}
	public LocalDate getHireDate() {
		return HireDate;
	}

	public void setHireDate(LocalDate HireDate) {
		this.HireDate = HireDate;
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
	public String getHomePhone() {
		return HomePhone;
	}

	public void setHomePhone(String HomePhone) {
		this.HomePhone = HomePhone;
	}
	public String getExtension() {
		return Extension;
	}

	public void setExtension(String Extension) {
		this.Extension = Extension;
	}
	public byte[] getPhoto() {
		return Photo;
	}

	public void setPhoto(byte[] Photo) {
		this.Photo = Photo;
	}
	public String getNotes() {
		return Notes;
	}

	public void setNotes(String Notes) {
		this.Notes = Notes;
	}
	public String getPhotoPath() {
		return PhotoPath;
	}

	public void setPhotoPath(String PhotoPath) {
		this.PhotoPath = PhotoPath;
	}
	public Double getSalary() {
		return Salary;
	}

	public void setSalary(Double Salary) {
		this.Salary = Salary;
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
