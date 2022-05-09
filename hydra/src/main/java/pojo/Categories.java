package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Categories extends LoggingPojo {

	private Integer categoryID;
	private String categoryName;
	private String description;
	private byte[] picture;

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
	public Categories(Integer categoryID,String categoryName,String description,byte[] picture) {
		this.categoryID = categoryID;
		this.categoryName = categoryName;
		this.description = description;
		this.picture = picture;
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
		boolean eqSimpleAttr = Objects.equals(categoryID,Categories.categoryID) && Objects.equals(categoryName,Categories.categoryName) && Objects.equals(description,Categories.description) && Objects.equals(picture,Categories.picture);
		boolean eqComplexAttr = false;
		eqComplexAttr = true && 
	Objects.equals(productList, Categories.productList) &&
 true;
		return eqSimpleAttr && eqComplexAttr;
	}
	
	@Override
	public String toString(){
		return "Categories { " + "categoryID="+categoryID +", "+
					"categoryName="+categoryName +", "+
					"description="+description +", "+
					"picture="+picture +"}"; 
	}
	
	public Integer getCategoryID() {
		return categoryID;
	}

	public void setCategoryID(Integer categoryID) {
		this.categoryID = categoryID;
	}
	public String getCategoryName() {
		return categoryName;
	}

	public void setCategoryName(String categoryName) {
		this.categoryName = categoryName;
	}
	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
	public byte[] getPicture() {
		return picture;
	}

	public void setPicture(byte[] picture) {
		this.picture = picture;
	}

	

	public List<Products> _getProductList() {
		return productList;
	}

	public void _setProductList(List<Products> productList) {
		this.productList = productList;
	}
}
