package tdo;

import pojo.Orders;
import java.util.List;
import java.util.ArrayList;

public class OrdersTDO extends Orders {
	private  String mongoDB_Orders_bought_CustomerRef; 
	public  String getMongoDB_Orders_bought_CustomerRef() {
		return this.mongoDB_Orders_bought_CustomerRef;
	}

	public void setMongoDB_Orders_bought_CustomerRef(  String mongoDB_Orders_bought_CustomerRef) {
		this.mongoDB_Orders_bought_CustomerRef = mongoDB_Orders_bought_CustomerRef;
	}

	private  String kvDB_CustomersPurchased_purchases_OrderID;
	public  String getKvDB_CustomersPurchased_purchases_OrderID() {
		return this.kvDB_CustomersPurchased_purchases_OrderID;
	}

	public void setKvDB_CustomersPurchased_purchases_OrderID(  String kvDB_CustomersPurchased_purchases_OrderID) {
		this.kvDB_CustomersPurchased_purchases_OrderID = kvDB_CustomersPurchased_purchases_OrderID;
	}

	private  String mongoDB_Orders_encoded_EmployeeRef; 
	public  String getMongoDB_Orders_encoded_EmployeeRef() {
		return this.mongoDB_Orders_encoded_EmployeeRef;
	}

	public void setMongoDB_Orders_encoded_EmployeeRef(  String mongoDB_Orders_encoded_EmployeeRef) {
		this.mongoDB_Orders_encoded_EmployeeRef = mongoDB_Orders_encoded_EmployeeRef;
	}

	private  String relDB_Order_Details_order_OrderID; 
	public  String getRelDB_Order_Details_order_OrderID() {
		return this.relDB_Order_Details_order_OrderID;
	}

	public void setRelDB_Order_Details_order_OrderID(  String relDB_Order_Details_order_OrderID) {
		this.relDB_Order_Details_order_OrderID = relDB_Order_Details_order_OrderID;
	}

}
