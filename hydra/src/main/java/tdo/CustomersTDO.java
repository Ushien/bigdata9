package tdo;

import pojo.Customers;
import java.util.List;
import java.util.ArrayList;

public class CustomersTDO extends Customers {
	private  String mongoDB_Orders_bought_custid;
	public  String getMongoDB_Orders_bought_custid() {
		return this.mongoDB_Orders_bought_custid;
	}

	public void setMongoDB_Orders_bought_custid(  String mongoDB_Orders_bought_custid) {
		this.mongoDB_Orders_bought_custid = mongoDB_Orders_bought_custid;
	}

	private ArrayList<String> kvDB_CustomersPurchased_purchases_orderref = new ArrayList<>(); 
	public ArrayList<String>  getKvDB_CustomersPurchased_purchases_orderref() {
		return this.kvDB_CustomersPurchased_purchases_orderref;
	}

	public void setKvDB_CustomersPurchased_purchases_orderref( ArrayList<String>  kvDB_CustomersPurchased_purchases_orderref) {
		this.kvDB_CustomersPurchased_purchases_orderref = kvDB_CustomersPurchased_purchases_orderref;
	}

}
