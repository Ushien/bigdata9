package hydra;

import org.apache.spark.sql.Dataset ;

import conditions.Condition;
import conditions.SuppliersAttribute;
import conditions.Operator;
import dao.impl.SuppliersServiceImpl;
import dao.services.SuppliersService;
import pojo.Suppliers;

public class Test {
	SuppliersService suppliersService = new SuppliersServiceImpl();
	Dataset<Supplier> supplierDataset;
	
	@org.junit.Test
	public void testGetAllSuppliers() {
		supplierDataset = suppliersService.getSuppliersList(null);
		supplierDataset.show(false);
	}
}