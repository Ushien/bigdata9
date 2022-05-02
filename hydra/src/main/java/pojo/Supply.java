package pojo;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class Supply extends LoggingPojo {

	private Products suppliedProduct;	
	private Suppliers supplier;	

	//Empty constructor
	public Supply() {}
	
	//Role constructor
	public Supply(Products suppliedProduct,Suppliers supplier){
		this.suppliedProduct=suppliedProduct;
		this.supplier=supplier;
	}
	

	@Override
    public Object clone() throws CloneNotSupportedException {
        Supply cloned = (Supply) super.clone();
		cloned.setSuppliedProduct((Products)cloned.getSuppliedProduct().clone());	
		cloned.setSupplier((Suppliers)cloned.getSupplier().clone());	
		return cloned;
    }

@Override
	public String toString(){
		return "supply : "+
				"roles : {" +  "suppliedProduct:Products={"+suppliedProduct+"},"+ 
					 "supplier:Suppliers={"+supplier+"}"+
				 "}"+
				"";
	}
	public Products getSuppliedProduct() {
		return suppliedProduct;
	}	

	public void setSuppliedProduct(Products suppliedProduct) {
		this.suppliedProduct = suppliedProduct;
	}
	
	public Suppliers getSupplier() {
		return supplier;
	}	

	public void setSupplier(Suppliers supplier) {
		this.supplier = supplier;
	}
	

}
