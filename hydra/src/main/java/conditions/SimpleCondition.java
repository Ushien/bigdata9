package conditions;

import pojo.*;

public class SimpleCondition<E> extends Condition<E> {

	private E attribute;
	private Operator operator;
	private Object value;

	public SimpleCondition(E attribute, Operator operator, Object value) {
		setAttribute(attribute);
		setOperator(operator);
		setValue(value);
	}

	public E getAttribute() {
		return this.attribute;
	}

	public void setAttribute(E attribute) {
		this.attribute = attribute;
	}

	public Operator getOperator() {
		return this.operator;
	}

	public void setOperator(Operator operator) {
		this.operator = operator;
	}

	public Object getValue() {
		return this.value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	@Override
	public boolean hasOrCondition() {
		return false;
	}

	@Override
	public Class<E> eval() throws Exception {
		if(getOperator() == null)
			throw new Exception("You cannot specify a NULL operator in a simple condition");
		if(getValue() == null && operator != Operator.EQUALS && operator != Operator.NOT_EQUALS)
			throw new Exception("You cannot specify a NULL value with this operator");

		return (Class<E>) attribute.getClass();
	}

	@Override
	public boolean evaluate(IPojo obj) {
		if(obj instanceof Orders)
			return evaluateOrders((Orders) obj);
		if(obj instanceof Products)
			return evaluateProducts((Products) obj);
		if(obj instanceof Suppliers)
			return evaluateSuppliers((Suppliers) obj);
		if(obj instanceof Customers)
			return evaluateCustomers((Customers) obj);
		if(obj instanceof Categories)
			return evaluateCategories((Categories) obj);
		if(obj instanceof Shippers)
			return evaluateShippers((Shippers) obj);
		if(obj instanceof Employees)
			return evaluateEmployees((Employees) obj);
		if(obj instanceof Region)
			return evaluateRegion((Region) obj);
		if(obj instanceof Territories)
			return evaluateTerritories((Territories) obj);
		if(obj instanceof ComposedOf)
			return evaluateComposedOf((ComposedOf) obj);
		return true;
	}


	private boolean evaluateOrders(Orders obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		OrdersAttribute attr = (OrdersAttribute) this.attribute;
		Object objectValue = null;

		if(attr == OrdersAttribute.id)
			objectValue = obj.getId();
		if(attr == OrdersAttribute.OrderDate)
			objectValue = obj.getOrderDate();
		if(attr == OrdersAttribute.RequiredDate)
			objectValue = obj.getRequiredDate();
		if(attr == OrdersAttribute.ShippedDate)
			objectValue = obj.getShippedDate();
		if(attr == OrdersAttribute.Freight)
			objectValue = obj.getFreight();
		if(attr == OrdersAttribute.ShipName)
			objectValue = obj.getShipName();
		if(attr == OrdersAttribute.ShipAddress)
			objectValue = obj.getShipAddress();
		if(attr == OrdersAttribute.ShipCity)
			objectValue = obj.getShipCity();
		if(attr == OrdersAttribute.ShipRegion)
			objectValue = obj.getShipRegion();
		if(attr == OrdersAttribute.ShipPostalCode)
			objectValue = obj.getShipPostalCode();
		if(attr == OrdersAttribute.ShipCountry)
			objectValue = obj.getShipCountry();

		return operator.evaluate(objectValue, this.getValue());
	}
	private boolean evaluateProducts(Products obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		ProductsAttribute attr = (ProductsAttribute) this.attribute;
		Object objectValue = null;

		if(attr == ProductsAttribute.productId)
			objectValue = obj.getProductId();
		if(attr == ProductsAttribute.ProductName)
			objectValue = obj.getProductName();
		if(attr == ProductsAttribute.QuantityPerUnit)
			objectValue = obj.getQuantityPerUnit();
		if(attr == ProductsAttribute.UnitPrice)
			objectValue = obj.getUnitPrice();
		if(attr == ProductsAttribute.UnitsInStock)
			objectValue = obj.getUnitsInStock();
		if(attr == ProductsAttribute.UnitsOnOrder)
			objectValue = obj.getUnitsOnOrder();
		if(attr == ProductsAttribute.ReorderLevel)
			objectValue = obj.getReorderLevel();
		if(attr == ProductsAttribute.Discontinued)
			objectValue = obj.getDiscontinued();

		return operator.evaluate(objectValue, this.getValue());
	}
	private boolean evaluateSuppliers(Suppliers obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		SuppliersAttribute attr = (SuppliersAttribute) this.attribute;
		Object objectValue = null;

		if(attr == SuppliersAttribute.supplierId)
			objectValue = obj.getSupplierId();
		if(attr == SuppliersAttribute.CompanyName)
			objectValue = obj.getCompanyName();
		if(attr == SuppliersAttribute.ContactName)
			objectValue = obj.getContactName();
		if(attr == SuppliersAttribute.ContactTitle)
			objectValue = obj.getContactTitle();
		if(attr == SuppliersAttribute.Address)
			objectValue = obj.getAddress();
		if(attr == SuppliersAttribute.City)
			objectValue = obj.getCity();
		if(attr == SuppliersAttribute.Region)
			objectValue = obj.getRegion();
		if(attr == SuppliersAttribute.PostalCode)
			objectValue = obj.getPostalCode();
		if(attr == SuppliersAttribute.Country)
			objectValue = obj.getCountry();
		if(attr == SuppliersAttribute.Phone)
			objectValue = obj.getPhone();
		if(attr == SuppliersAttribute.Fax)
			objectValue = obj.getFax();
		if(attr == SuppliersAttribute.HomePage)
			objectValue = obj.getHomePage();

		return operator.evaluate(objectValue, this.getValue());
	}
	private boolean evaluateCustomers(Customers obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		CustomersAttribute attr = (CustomersAttribute) this.attribute;
		Object objectValue = null;

		if(attr == CustomersAttribute.customerID)
			objectValue = obj.getCustomerID();
		if(attr == CustomersAttribute.CompanyName)
			objectValue = obj.getCompanyName();
		if(attr == CustomersAttribute.ContactName)
			objectValue = obj.getContactName();
		if(attr == CustomersAttribute.ContactTitle)
			objectValue = obj.getContactTitle();
		if(attr == CustomersAttribute.Address)
			objectValue = obj.getAddress();
		if(attr == CustomersAttribute.City)
			objectValue = obj.getCity();
		if(attr == CustomersAttribute.Region)
			objectValue = obj.getRegion();
		if(attr == CustomersAttribute.PostalCode)
			objectValue = obj.getPostalCode();
		if(attr == CustomersAttribute.Country)
			objectValue = obj.getCountry();
		if(attr == CustomersAttribute.Phone)
			objectValue = obj.getPhone();
		if(attr == CustomersAttribute.Fax)
			objectValue = obj.getFax();

		return operator.evaluate(objectValue, this.getValue());
	}
	private boolean evaluateCategories(Categories obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		CategoriesAttribute attr = (CategoriesAttribute) this.attribute;
		Object objectValue = null;

		if(attr == CategoriesAttribute.categoryID)
			objectValue = obj.getCategoryID();
		if(attr == CategoriesAttribute.CategoryName)
			objectValue = obj.getCategoryName();
		if(attr == CategoriesAttribute.Description)
			objectValue = obj.getDescription();
		if(attr == CategoriesAttribute.Picture)
			objectValue = obj.getPicture();

		return operator.evaluate(objectValue, this.getValue());
	}
	private boolean evaluateShippers(Shippers obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		ShippersAttribute attr = (ShippersAttribute) this.attribute;
		Object objectValue = null;

		if(attr == ShippersAttribute.shipperID)
			objectValue = obj.getShipperID();
		if(attr == ShippersAttribute.CompanyName)
			objectValue = obj.getCompanyName();
		if(attr == ShippersAttribute.Phone)
			objectValue = obj.getPhone();

		return operator.evaluate(objectValue, this.getValue());
	}
	private boolean evaluateEmployees(Employees obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		EmployeesAttribute attr = (EmployeesAttribute) this.attribute;
		Object objectValue = null;

		if(attr == EmployeesAttribute.employeeID)
			objectValue = obj.getEmployeeID();
		if(attr == EmployeesAttribute.LastName)
			objectValue = obj.getLastName();
		if(attr == EmployeesAttribute.FirstName)
			objectValue = obj.getFirstName();
		if(attr == EmployeesAttribute.Title)
			objectValue = obj.getTitle();
		if(attr == EmployeesAttribute.TitleOfCourtesy)
			objectValue = obj.getTitleOfCourtesy();
		if(attr == EmployeesAttribute.BirthDate)
			objectValue = obj.getBirthDate();
		if(attr == EmployeesAttribute.HireDate)
			objectValue = obj.getHireDate();
		if(attr == EmployeesAttribute.Address)
			objectValue = obj.getAddress();
		if(attr == EmployeesAttribute.City)
			objectValue = obj.getCity();
		if(attr == EmployeesAttribute.Region)
			objectValue = obj.getRegion();
		if(attr == EmployeesAttribute.PostalCode)
			objectValue = obj.getPostalCode();
		if(attr == EmployeesAttribute.Country)
			objectValue = obj.getCountry();
		if(attr == EmployeesAttribute.HomePhone)
			objectValue = obj.getHomePhone();
		if(attr == EmployeesAttribute.Extension)
			objectValue = obj.getExtension();
		if(attr == EmployeesAttribute.Photo)
			objectValue = obj.getPhoto();
		if(attr == EmployeesAttribute.Notes)
			objectValue = obj.getNotes();
		if(attr == EmployeesAttribute.PhotoPath)
			objectValue = obj.getPhotoPath();
		if(attr == EmployeesAttribute.Salary)
			objectValue = obj.getSalary();

		return operator.evaluate(objectValue, this.getValue());
	}
	private boolean evaluateRegion(Region obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		RegionAttribute attr = (RegionAttribute) this.attribute;
		Object objectValue = null;

		if(attr == RegionAttribute.regionID)
			objectValue = obj.getRegionID();
		if(attr == RegionAttribute.RegionDescription)
			objectValue = obj.getRegionDescription();

		return operator.evaluate(objectValue, this.getValue());
	}
	private boolean evaluateTerritories(Territories obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		TerritoriesAttribute attr = (TerritoriesAttribute) this.attribute;
		Object objectValue = null;

		if(attr == TerritoriesAttribute.territoryID)
			objectValue = obj.getTerritoryID();
		if(attr == TerritoriesAttribute.TerritoryDescription)
			objectValue = obj.getTerritoryDescription();

		return operator.evaluate(objectValue, this.getValue());
	}
		private boolean evaluateComposedOf(ComposedOf obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		ComposedOfAttribute attr = (ComposedOfAttribute) this.attribute;
		Object objectValue = null;

		if(attr == ComposedOfAttribute.UnitPrice)
			objectValue = obj.getUnitPrice();
		if(attr == ComposedOfAttribute.Quantity)
			objectValue = obj.getQuantity();
		if(attr == ComposedOfAttribute.Discount)
			objectValue = obj.getDiscount();

		return operator.evaluate(objectValue, this.getValue());
	}

	
}
