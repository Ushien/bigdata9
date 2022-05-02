package tdo;

import pojo.Categories;
import java.util.List;
import java.util.ArrayList;

public class CategoriesTDO extends Categories {
	private  String relDB_Products_isCategory_CategoryID;
	public  String getRelDB_Products_isCategory_CategoryID() {
		return this.relDB_Products_isCategory_CategoryID;
	}

	public void setRelDB_Products_isCategory_CategoryID(  String relDB_Products_isCategory_CategoryID) {
		this.relDB_Products_isCategory_CategoryID = relDB_Products_isCategory_CategoryID;
	}

}
