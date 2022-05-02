package example;

import org.apache.spark.sql.Dataset;
import org.junit.jupiter.api.Test;

import conditions.Condition;
import conditions.SuppliersAttribute;
import conditions.Operator;
import dao.impl.SuppliersServiceImpl;
import dao.services.SuppliersService;
import pojo.Suppliers;

public class suppliers {
	SuppliersService suppliersService = new SuppliersServiceImpl();
	util.Dataset<Suppliers> supplierDataset;
	
	@Test
	public void testGetAllSuppliers() {
		supplierDataset = suppliersService.getSuppliersList(null);
		supplierDataset.show();
	}
}