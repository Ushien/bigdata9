package tdo;

import pojo.Employees;
import java.util.List;
import java.util.ArrayList;

public class EmployeesTDO extends Employees {
	private  String relDB_EmployeeTerritories_employee_EmployeeID; 
	public  String getRelDB_EmployeeTerritories_employee_EmployeeID() {
		return this.relDB_EmployeeTerritories_employee_EmployeeID;
	}

	public void setRelDB_EmployeeTerritories_employee_EmployeeID(  String relDB_EmployeeTerritories_employee_EmployeeID) {
		this.relDB_EmployeeTerritories_employee_EmployeeID = relDB_EmployeeTerritories_employee_EmployeeID;
	}

	private  String relDB_Employees_manager_ReportsTo; 
	public  String getRelDB_Employees_manager_ReportsTo() {
		return this.relDB_Employees_manager_ReportsTo;
	}

	public void setRelDB_Employees_manager_ReportsTo(  String relDB_Employees_manager_ReportsTo) {
		this.relDB_Employees_manager_ReportsTo = relDB_Employees_manager_ReportsTo;
	}

	private  String relDB_Employees_manager_EmployeeID;
	public  String getRelDB_Employees_manager_EmployeeID() {
		return this.relDB_Employees_manager_EmployeeID;
	}

	public void setRelDB_Employees_manager_EmployeeID(  String relDB_Employees_manager_EmployeeID) {
		this.relDB_Employees_manager_EmployeeID = relDB_Employees_manager_EmployeeID;
	}

	private  String mongoDB_Orders_encoded_EmployeeID;
	public  String getMongoDB_Orders_encoded_EmployeeID() {
		return this.mongoDB_Orders_encoded_EmployeeID;
	}

	public void setMongoDB_Orders_encoded_EmployeeID(  String mongoDB_Orders_encoded_EmployeeID) {
		this.mongoDB_Orders_encoded_EmployeeID = mongoDB_Orders_encoded_EmployeeID;
	}

}
