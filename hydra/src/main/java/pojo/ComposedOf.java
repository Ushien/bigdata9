package pojo;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class ComposedOf extends LoggingPojo {

	private Orders order;	
	private Products orderedProducts;	
	private Double UnitPrice;	
	private Integer Quantity;	
	private Double Discount;	

	//Empty constructor
	public ComposedOf() {}
	
	//Role constructor
	public ComposedOf(Orders order,Products orderedProducts){
		this.order=order;
		this.orderedProducts=orderedProducts;
	}
	

	@Override
    public Object clone() throws CloneNotSupportedException {
        ComposedOf cloned = (ComposedOf) super.clone();
		cloned.setOrder((Orders)cloned.getOrder().clone());	
		cloned.setOrderedProducts((Products)cloned.getOrderedProducts().clone());	
		return cloned;
    }

@Override
	public String toString(){
		return "composedOf : "+
				"roles : {" +  "order:Orders={"+order+"},"+ 
					 "orderedProducts:Products={"+orderedProducts+"}"+
				 "}"+
			" attributes : { " + "UnitPrice="+UnitPrice +", "+
					"Quantity="+Quantity +", "+
					"Discount="+Discount +"}"; 
	}
	public Orders getOrder() {
		return order;
	}	

	public void setOrder(Orders order) {
		this.order = order;
	}
	
	public Products getOrderedProducts() {
		return orderedProducts;
	}	

	public void setOrderedProducts(Products orderedProducts) {
		this.orderedProducts = orderedProducts;
	}
	
	public Double getUnitPrice() {
		return UnitPrice;
	}

	public void setUnitPrice(Double UnitPrice) {
		this.UnitPrice = UnitPrice;
	}
	
	public Integer getQuantity() {
		return Quantity;
	}

	public void setQuantity(Integer Quantity) {
		this.Quantity = Quantity;
	}
	
	public Double getDiscount() {
		return Discount;
	}

	public void setDiscount(Double Discount) {
		this.Discount = Discount;
	}
	

}
