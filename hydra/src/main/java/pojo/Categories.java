package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Categories extends LoggingPojo {

	private Integer categoryID;
	private String CategoryName;
	private String Description;
	private byte[] Picture;

	public enum typeOf {
		category
	}
	private List<Products> productList;

	// Empty constructor
	public Categories() {}

	// Constructor on Identifier
	public Categories(Integer categoryID){
		this.categoryID = categoryID;
	}
	/*
	* Constructor on simple attribute 
	*/
	public Categories(Integer categoryID,String CategoryName,String Description,byte[] Picture) {
		this.categoryID = categoryID;
		this.CategoryName = CategoryName;
		this.Description = Description;
		this.Picture = Picture;
	}
	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

	@Override
	public boolean equals(Object o){
		if(this==o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Categories Categories = (Categories) o;
		boolean eqSimpleAttr = Objects.equals(categoryID,Categories.categoryID) && Objects.equals(CategoryName,Categories.CategoryName) && Objects.equals(Description,Categories.Description) && Objects.equals(Picture,Categories.Picture);
		boolean eqComplexAttr = false;
		eqComplexAttr = true && 
	Objects.equals(productList, Categories.productList) &&
 true;
		return eqSimpleAttr && eqComplexAttr;
	}
	
	@Override
	public String toString(){
		return "Categories { " + "categoryID="+categoryID +", "+
					"CategoryName="+CategoryName +", "+
					"Description="+Description +", "+
					"Picture="+Picture +"}"; 
	}
	
	public Integer getCategoryID() {
		return categoryID;
	}

	public void setCategoryID(Integer categoryID) {
		this.categoryID = categoryID;
	}
	public String getCategoryName() {
		return CategoryName;
	}

	public void setCategoryName(String CategoryName) {
		this.CategoryName = CategoryName;
	}
	public String getDescription() {
		return Description;
	}

	public void setDescription(String Description) {
		this.Description = Description;
	}
	public byte[] getPicture() {
		return Picture;
	}

	public void setPicture(byte[] Picture) {
		this.Picture = Picture;
	}

	

	public List<Products> _getProductList() {
		return productList;
	}

	public void _setProductList(List<Products> productList) {
		this.productList = productList;
	}
}
