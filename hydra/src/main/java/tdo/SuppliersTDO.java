package tdo;

import pojo.Suppliers;
import java.util.List;
import java.util.ArrayList;

public class SuppliersTDO extends Suppliers {
	private  String relDB_Products_supply_SupplierID;
	public  String getRelDB_Products_supply_SupplierID() {
		return this.relDB_Products_supply_SupplierID;
	}

	public void setRelDB_Products_supply_SupplierID(  String relDB_Products_supply_SupplierID) {
		this.relDB_Products_supply_SupplierID = relDB_Products_supply_SupplierID;
	}

}
