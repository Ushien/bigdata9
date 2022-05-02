package example;

import org.apache.spark.sql.Dataset;
import org.junit.jupiter.api.Test;

import conditions.Condition;
import conditions.ProductsAttribute;
import conditions.Operator;
import dao.impl.ProductsServiceImpl;
import dao.services.ProductsService;
import pojo.Products;

public class test {
	
	ProductsService productsService = new ProductsServiceImpl();
	util.Dataset<Products> productsDataset;
	
	@Test
	public void testGetAllProducts() {
		productsDataset = productsService.getProductsList(null);
		productsDataset.show();
	}
	
}

