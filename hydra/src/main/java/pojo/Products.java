package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Products extends LoggingPojo {

	private Integer productId;
	private String productName;
	private String quantityPerUnit;
	private Double unitPrice;
	private Integer unitsInStock;
	private Integer unitsOnOrder;
	private Integer reorderLevel;
	private Boolean discontinued;

	public enum supply {
		suppliedProduct
	}
	private Suppliers supplier;
	public enum typeOf {
		product
	}
	private Categories category;
	private List<ComposedOf> composedOfListAsOrderedProducts;

	// Empty constructor
	public Products() {}

	// Constructor on Identifier
	public Products(Integer productId){
		this.productId = productId;
	}
	/*
	* Constructor on simple attribute 
	*/
	public Products(Integer productId,String productName,String quantityPerUnit,Double unitPrice,Integer unitsInStock,Integer unitsOnOrder,Integer reorderLevel,Boolean discontinued) {
		this.productId = productId;
		this.productName = productName;
		this.quantityPerUnit = quantityPerUnit;
		this.unitPrice = unitPrice;
		this.unitsInStock = unitsInStock;
		this.unitsOnOrder = unitsOnOrder;
		this.reorderLevel = reorderLevel;
		this.discontinued = discontinued;
	}
	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

	@Override
	public boolean equals(Object o){
		if(this==o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Products Products = (Products) o;
		boolean eqSimpleAttr = Objects.equals(productId,Products.productId) && Objects.equals(productName,Products.productName) && Objects.equals(quantityPerUnit,Products.quantityPerUnit) && Objects.equals(unitPrice,Products.unitPrice) && Objects.equals(unitsInStock,Products.unitsInStock) && Objects.equals(unitsOnOrder,Products.unitsOnOrder) && Objects.equals(reorderLevel,Products.reorderLevel) && Objects.equals(discontinued,Products.discontinued);
		boolean eqComplexAttr = false;
		eqComplexAttr = true && 
	Objects.equals(supplier, Products.supplier) &&
	Objects.equals(category, Products.category) &&
	Objects.equals(composedOfListAsOrderedProducts,Products.composedOfListAsOrderedProducts) &&
 true;
		return eqSimpleAttr && eqComplexAttr;
	}
	
	@Override
	public String toString(){
		return "Products { " + "productId="+productId +", "+
					"productName="+productName +", "+
					"quantityPerUnit="+quantityPerUnit +", "+
					"unitPrice="+unitPrice +", "+
					"unitsInStock="+unitsInStock +", "+
					"unitsOnOrder="+unitsOnOrder +", "+
					"reorderLevel="+reorderLevel +", "+
					"discontinued="+discontinued +"}"; 
	}
	
	public Integer getProductId() {
		return productId;
	}

	public void setProductId(Integer productId) {
		this.productId = productId;
	}
	public String getProductName() {
		return productName;
	}

	public void setProductName(String productName) {
		this.productName = productName;
	}
	public String getQuantityPerUnit() {
		return quantityPerUnit;
	}

	public void setQuantityPerUnit(String quantityPerUnit) {
		this.quantityPerUnit = quantityPerUnit;
	}
	public Double getUnitPrice() {
		return unitPrice;
	}

	public void setUnitPrice(Double unitPrice) {
		this.unitPrice = unitPrice;
	}
	public Integer getUnitsInStock() {
		return unitsInStock;
	}

	public void setUnitsInStock(Integer unitsInStock) {
		this.unitsInStock = unitsInStock;
	}
	public Integer getUnitsOnOrder() {
		return unitsOnOrder;
	}

	public void setUnitsOnOrder(Integer unitsOnOrder) {
		this.unitsOnOrder = unitsOnOrder;
	}
	public Integer getReorderLevel() {
		return reorderLevel;
	}

	public void setReorderLevel(Integer reorderLevel) {
		this.reorderLevel = reorderLevel;
	}
	public Boolean getDiscontinued() {
		return discontinued;
	}

	public void setDiscontinued(Boolean discontinued) {
		this.discontinued = discontinued;
	}

	

	public Suppliers _getSupplier() {
		return supplier;
	}

	public void _setSupplier(Suppliers supplier) {
		this.supplier = supplier;
	}
	public Categories _getCategory() {
		return category;
	}

	public void _setCategory(Categories category) {
		this.category = category;
	}
	public java.util.List<ComposedOf> _getComposedOfListAsOrderedProducts() {
		return composedOfListAsOrderedProducts;
	}

	public void _setComposedOfListAsOrderedProducts(java.util.List<ComposedOf> composedOfListAsOrderedProducts) {
		this.composedOfListAsOrderedProducts = composedOfListAsOrderedProducts;
	}
}
