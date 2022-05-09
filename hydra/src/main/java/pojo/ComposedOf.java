package pojo;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class ComposedOf extends LoggingPojo {

	private Orders order;	
	private Products orderedProducts;	
	private Double unitPrice;	
	private Integer quantity;	
	private Double discount;	

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
			" attributes : { " + "unitPrice="+unitPrice +", "+
					"quantity="+quantity +", "+
					"discount="+discount +"}"; 
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
		return unitPrice;
	}

	public void setUnitPrice(Double unitPrice) {
		this.unitPrice = unitPrice;
	}
	
	public Integer getQuantity() {
		return quantity;
	}

	public void setQuantity(Integer quantity) {
		this.quantity = quantity;
	}
	
	public Double getDiscount() {
		return discount;
	}

	public void setDiscount(Double discount) {
		this.discount = discount;
	}
	

}
