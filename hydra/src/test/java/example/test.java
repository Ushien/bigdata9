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

public class test {
	
	ProductsService productsService = new ProductsServiceImpl();
	util.Dataset<Products> productsDataset;
	
	CustomersService customersService = new CustomersServiceImpl();
	util.Dataset<Customers> customersDataset;
	
	EmployeesService employeesService = new EmployeesServiceImpl();
	util.Dataset<Employees> employeesDataset;
	
	OrdersService ordersService = new OrdersServiceImpl();
	util.Dataset<Orders> ordersDataset;
	
	public void testGetAllProducts() {
		productsDataset = productsService.getProductsList(null);
		productsDataset.show();
	}
	
	@Test
	public void testGetCustomers() {
		SimpleCondition<EmployeesAttribute> margaretCondition = new SimpleCondition<>(EmployeesAttribute.FirstName, Operator.EQUALS, "Margaret");
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
	
}

