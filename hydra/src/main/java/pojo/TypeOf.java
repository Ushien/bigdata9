package pojo;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class TypeOf extends LoggingPojo {

	private Products product;	
	private Categories category;	

	//Empty constructor
	public TypeOf() {}
	
	//Role constructor
	public TypeOf(Products product,Categories category){
		this.product=product;
		this.category=category;
	}
	

	@Override
    public Object clone() throws CloneNotSupportedException {
        TypeOf cloned = (TypeOf) super.clone();
		cloned.setProduct((Products)cloned.getProduct().clone());	
		cloned.setCategory((Categories)cloned.getCategory().clone());	
		return cloned;
    }

@Override
	public String toString(){
		return "typeOf : "+
				"roles : {" +  "product:Products={"+product+"},"+ 
					 "category:Categories={"+category+"}"+
				 "}"+
				"";
	}
	public Products getProduct() {
		return product;
	}	

	public void setProduct(Products product) {
		this.product = product;
	}
	
	public Categories getCategory() {
		return category;
	}	

	public void setCategory(Categories category) {
		this.category = category;
	}
	

}
