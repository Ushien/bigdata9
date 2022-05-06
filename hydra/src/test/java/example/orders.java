package example;

import org.apache.spark.sql.Dataset;
import org.junit.jupiter.api.Test;

import conditions.*;

import pojo.Customers;
import pojo.Orders;
import pojo.Buy;

import dao.impl.CustomersServiceImpl;
import dao.impl.BuyServiceImpl;
import dao.impl.OrdersServiceImpl;

import dao.services.BuyService;
import dao.services.CustomersService;
import dao.services.OrdersService;

//import java.util.HashSet;
//import java.util.List;
//import java.util.Set;

public class orders {
	CustomersService customersService = new CustomersServiceImpl();
	util.Dataset<Customers> customersDataset;
	OrdersService ordersService = new OrdersServiceImpl();
	util.Dataset<Orders> ordersDataset;
	BuyService buyService = new BuyServiceImpl();
	util.Dataset<Buy> buyDataset;
	
	@Test
	public void testGetOrdersbyCustomer() {
		SimpleCondition<CustomersAttribute> customerCondition = conditions.Condition.simple(CustomersAttribute.Address, Operator.EQUALS, "Av. dos Lusadas, 23");
		util.Dataset<Customers> customer = customersService.getCustomersList(customerCondition);
		Customers lusadas = customer.collectAsList().get(0);
		util.Dataset<Orders> order = ordersService.getOrdersList(Orders.buy.boughtOrder, lusadas);
		order.show();
	}
}