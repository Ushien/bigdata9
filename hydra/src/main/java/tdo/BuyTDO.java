	package tdo;

	import pojo.Buy;
	
	public class BuyTDO extends Buy {
	
	private String mongoDB_Orders_bought_CustomerRef;
	public String getMongoDB_Orders_bought_CustomerRef() {
		return this.mongoDB_Orders_bought_CustomerRef;
	}

	public void setMongoDB_Orders_bought_CustomerRef( String mongoDB_Orders_bought_CustomerRef) {
		this.mongoDB_Orders_bought_CustomerRef = mongoDB_Orders_bought_CustomerRef;
	}

	private String mongoDB_Orders_bought_custid;
	public String getMongoDB_Orders_bought_custid() {
		return this.mongoDB_Orders_bought_custid;
	}

	public void setMongoDB_Orders_bought_custid( String mongoDB_Orders_bought_custid) {
		this.mongoDB_Orders_bought_custid = mongoDB_Orders_bought_custid;
	}

	private String kvDB_CustomersPurchased_purchases_orderref;
	public String getKvDB_CustomersPurchased_purchases_orderref() {
		return this.kvDB_CustomersPurchased_purchases_orderref;
	}

	public void setKvDB_CustomersPurchased_purchases_orderref( String kvDB_CustomersPurchased_purchases_orderref) {
		this.kvDB_CustomersPurchased_purchases_orderref = kvDB_CustomersPurchased_purchases_orderref;
	}

	private String kvDB_CustomersPurchased_purchases_OrderID;
	public String getKvDB_CustomersPurchased_purchases_OrderID() {
		return this.kvDB_CustomersPurchased_purchases_OrderID;
	}

	public void setKvDB_CustomersPurchased_purchases_OrderID( String kvDB_CustomersPurchased_purchases_OrderID) {
		this.kvDB_CustomersPurchased_purchases_OrderID = kvDB_CustomersPurchased_purchases_OrderID;
	}

	
	}
