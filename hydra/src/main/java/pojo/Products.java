package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Products extends LoggingPojo {

	private Integer productId;
	private String ProductName;
	private String QuantityPerUnit;
	private Double UnitPrice;
	private Integer UnitsInStock;
	private Integer UnitsOnOrder;
	private Integer ReorderLevel;
	private Boolean Discontinued;

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
	public Products(Integer productId,String ProductName,String QuantityPerUnit,Double UnitPrice,Integer UnitsInStock,Integer UnitsOnOrder,Integer ReorderLevel,Boolean Discontinued) {
		this.productId = productId;
		this.ProductName = ProductName;
		this.QuantityPerUnit = QuantityPerUnit;
		this.UnitPrice = UnitPrice;
		this.UnitsInStock = UnitsInStock;
		this.UnitsOnOrder = UnitsOnOrder;
		this.ReorderLevel = ReorderLevel;
		this.Discontinued = Discontinued;
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
		boolean eqSimpleAttr = Objects.equals(productId,Products.productId) && Objects.equals(ProductName,Products.ProductName) && Objects.equals(QuantityPerUnit,Products.QuantityPerUnit) && Objects.equals(UnitPrice,Products.UnitPrice) && Objects.equals(UnitsInStock,Products.UnitsInStock) && Objects.equals(UnitsOnOrder,Products.UnitsOnOrder) && Objects.equals(ReorderLevel,Products.ReorderLevel) && Objects.equals(Discontinued,Products.Discontinued);
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
					"ProductName="+ProductName +", "+
					"QuantityPerUnit="+QuantityPerUnit +", "+
					"UnitPrice="+UnitPrice +", "+
					"UnitsInStock="+UnitsInStock +", "+
					"UnitsOnOrder="+UnitsOnOrder +", "+
					"ReorderLevel="+ReorderLevel +", "+
					"Discontinued="+Discontinued +"}"; 
	}
	
	public Integer getProductId() {
		return productId;
	}

	public void setProductId(Integer productId) {
		this.productId = productId;
	}
	public String getProductName() {
		return ProductName;
	}

	public void setProductName(String ProductName) {
		this.ProductName = ProductName;
	}
	public String getQuantityPerUnit() {
		return QuantityPerUnit;
	}

	public void setQuantityPerUnit(String QuantityPerUnit) {
		this.QuantityPerUnit = QuantityPerUnit;
	}
	public Double getUnitPrice() {
		return UnitPrice;
	}

	public void setUnitPrice(Double UnitPrice) {
		this.UnitPrice = UnitPrice;
	}
	public Integer getUnitsInStock() {
		return UnitsInStock;
	}

	public void setUnitsInStock(Integer UnitsInStock) {
		this.UnitsInStock = UnitsInStock;
	}
	public Integer getUnitsOnOrder() {
		return UnitsOnOrder;
	}

	public void setUnitsOnOrder(Integer UnitsOnOrder) {
		this.UnitsOnOrder = UnitsOnOrder;
	}
	public Integer getReorderLevel() {
		return ReorderLevel;
	}

	public void setReorderLevel(Integer ReorderLevel) {
		this.ReorderLevel = ReorderLevel;
	}
	public Boolean getDiscontinued() {
		return Discontinued;
	}

	public void setDiscontinued(Boolean Discontinued) {
		this.Discontinued = Discontinued;
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
