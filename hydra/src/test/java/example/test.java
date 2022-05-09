package example;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.junit.jupiter.api.Test;

import conditions.*;
import dao.impl.*;
import dao.services.*;
import pojo.*;
import scala.Console;
import dao.impl.ProductsServiceImpl;
import dao.services.ProductsService;
import pojo.Products;

import dao.impl.SuppliersServiceImpl;
import dao.services.SuppliersService;
import pojo.Suppliers;

import pojo.Customers;
import pojo.Orders;
import pojo.Buy;

import dao.impl.CustomersServiceImpl;
import dao.impl.BuyServiceImpl;
import dao.impl.OrdersServiceImpl;

import dao.services.BuyService;
import dao.services.CustomersService;
import dao.services.OrdersService;

public class test {
	
	ProductsService productsService = new ProductsServiceImpl();
	util.Dataset<Products> productsDataset;
	
	CustomersService customersService = new CustomersServiceImpl();
	util.Dataset<Customers> customersDataset;
	
	EmployeesService employeesService = new EmployeesServiceImpl();
	util.Dataset<Employees> employeesDataset;
	
	OrdersService ordersService = new OrdersServiceImpl();
	util.Dataset<Orders> ordersDataset;
	
	SuppliersService suppliersService = new SuppliersServiceImpl();
	util.Dataset<Suppliers> supplierDataset;
	
	BuyService buyService = new BuyServiceImpl();
	util.Dataset<Buy> buyDataset;
	
	@Test
	public void testGetAllProducts() {
		productsDataset = productsService.getProductsList(null);
		productsDataset.show();
	}
	
	@Test
	public void testGetCustomers() {
		SimpleCondition<EmployeesAttribute> margaretCondition = new SimpleCondition<>(EmployeesAttribute.firstName, Operator.EQUALS, "Margaret");
		util.Dataset<Employees> employees = employeesService.getEmployeesList(margaretCondition);
		employees.show();
		
		Employees margaret = employees.collectAsList().get(0);		
		util.Dataset<Orders> orders = ordersService.getOrdersList(Orders.register.processedOrder, margaret);
		orders.show();
	
		Set<Customers> customersSet = new HashSet<>();
		orders.collectAsList().forEach(m -> {
			SimpleCondition<OrdersAttribute> ordersCondition = new SimpleCondition<>(OrdersAttribute.id, Operator.EQUALS, m.getId());
			List<Customers> customers = customersService.getCustomersList(Customers.buy.customer, ordersCondition).collectAsList();
			customersSet.addAll(customers);
		});
		customersSet.forEach(System.out::println);
		System.out.println(customersSet.size());
		
		}
	
	public void testGetAllSuppliers() {
		supplierDataset = suppliersService.getSuppliersList(null);
		supplierDataset.show();
	}
	
	@Test
	public void testGetOrdersbyCustomer() {
		SimpleCondition<CustomersAttribute> customerCondition = Condition.simple(CustomersAttribute.address, Operator.EQUALS, "Av. dos Lusadas, 23");
		util.Dataset<Customers> customer = customersService.getCustomersList(customerCondition);
		//customer.show();
		Customers lusadas = customer.collectAsList().get(0);
		util.Dataset<Orders> order = ordersService.getOrdersList(Orders.buy.boughtOrder, lusadas);
		order.show();
	}
}

