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
		if(attr == OrdersAttribute.orderDate)
			objectValue = obj.getOrderDate();
		if(attr == OrdersAttribute.requiredDate)
			objectValue = obj.getRequiredDate();
		if(attr == OrdersAttribute.shippedDate)
			objectValue = obj.getShippedDate();
		if(attr == OrdersAttribute.freight)
			objectValue = obj.getFreight();
		if(attr == OrdersAttribute.shipName)
			objectValue = obj.getShipName();
		if(attr == OrdersAttribute.shipAddress)
			objectValue = obj.getShipAddress();
		if(attr == OrdersAttribute.shipCity)
			objectValue = obj.getShipCity();
		if(attr == OrdersAttribute.shipRegion)
			objectValue = obj.getShipRegion();
		if(attr == OrdersAttribute.shipPostalCode)
			objectValue = obj.getShipPostalCode();
		if(attr == OrdersAttribute.shipCountry)
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
		if(attr == ProductsAttribute.productName)
			objectValue = obj.getProductName();
		if(attr == ProductsAttribute.quantityPerUnit)
			objectValue = obj.getQuantityPerUnit();
		if(attr == ProductsAttribute.unitPrice)
			objectValue = obj.getUnitPrice();
		if(attr == ProductsAttribute.unitsInStock)
			objectValue = obj.getUnitsInStock();
		if(attr == ProductsAttribute.unitsOnOrder)
			objectValue = obj.getUnitsOnOrder();
		if(attr == ProductsAttribute.reorderLevel)
			objectValue = obj.getReorderLevel();
		if(attr == ProductsAttribute.discontinued)
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
		if(attr == SuppliersAttribute.companyName)
			objectValue = obj.getCompanyName();
		if(attr == SuppliersAttribute.contactName)
			objectValue = obj.getContactName();
		if(attr == SuppliersAttribute.contactTitle)
			objectValue = obj.getContactTitle();
		if(attr == SuppliersAttribute.address)
			objectValue = obj.getAddress();
		if(attr == SuppliersAttribute.city)
			objectValue = obj.getCity();
		if(attr == SuppliersAttribute.region)
			objectValue = obj.getRegion();
		if(attr == SuppliersAttribute.postalCode)
			objectValue = obj.getPostalCode();
		if(attr == SuppliersAttribute.country)
			objectValue = obj.getCountry();
		if(attr == SuppliersAttribute.phone)
			objectValue = obj.getPhone();
		if(attr == SuppliersAttribute.fax)
			objectValue = obj.getFax();
		if(attr == SuppliersAttribute.homePage)
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
		if(attr == CustomersAttribute.companyName)
			objectValue = obj.getCompanyName();
		if(attr == CustomersAttribute.contactName)
			objectValue = obj.getContactName();
		if(attr == CustomersAttribute.contactTitle)
			objectValue = obj.getContactTitle();
		if(attr == CustomersAttribute.address)
			objectValue = obj.getAddress();
		if(attr == CustomersAttribute.city)
			objectValue = obj.getCity();
		if(attr == CustomersAttribute.region)
			objectValue = obj.getRegion();
		if(attr == CustomersAttribute.postalCode)
			objectValue = obj.getPostalCode();
		if(attr == CustomersAttribute.country)
			objectValue = obj.getCountry();
		if(attr == CustomersAttribute.phone)
			objectValue = obj.getPhone();
		if(attr == CustomersAttribute.fax)
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
		if(attr == CategoriesAttribute.categoryName)
			objectValue = obj.getCategoryName();
		if(attr == CategoriesAttribute.description)
			objectValue = obj.getDescription();
		if(attr == CategoriesAttribute.picture)
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
		if(attr == ShippersAttribute.companyName)
			objectValue = obj.getCompanyName();
		if(attr == ShippersAttribute.phone)
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
		if(attr == EmployeesAttribute.lastName)
			objectValue = obj.getLastName();
		if(attr == EmployeesAttribute.firstName)
			objectValue = obj.getFirstName();
		if(attr == EmployeesAttribute.title)
			objectValue = obj.getTitle();
		if(attr == EmployeesAttribute.titleOfCourtesy)
			objectValue = obj.getTitleOfCourtesy();
		if(attr == EmployeesAttribute.birthDate)
			objectValue = obj.getBirthDate();
		if(attr == EmployeesAttribute.hireDate)
			objectValue = obj.getHireDate();
		if(attr == EmployeesAttribute.address)
			objectValue = obj.getAddress();
		if(attr == EmployeesAttribute.city)
			objectValue = obj.getCity();
		if(attr == EmployeesAttribute.region)
			objectValue = obj.getRegion();
		if(attr == EmployeesAttribute.postalCode)
			objectValue = obj.getPostalCode();
		if(attr == EmployeesAttribute.country)
			objectValue = obj.getCountry();
		if(attr == EmployeesAttribute.homePhone)
			objectValue = obj.getHomePhone();
		if(attr == EmployeesAttribute.extension)
			objectValue = obj.getExtension();
		if(attr == EmployeesAttribute.photo)
			objectValue = obj.getPhoto();
		if(attr == EmployeesAttribute.notes)
			objectValue = obj.getNotes();
		if(attr == EmployeesAttribute.photoPath)
			objectValue = obj.getPhotoPath();
		if(attr == EmployeesAttribute.salary)
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
		if(attr == RegionAttribute.regionDescription)
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
		if(attr == TerritoriesAttribute.territoryDescription)
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

		if(attr == ComposedOfAttribute.unitPrice)
			objectValue = obj.getUnitPrice();
		if(attr == ComposedOfAttribute.quantity)
			objectValue = obj.getQuantity();
		if(attr == ComposedOfAttribute.discount)
			objectValue = obj.getDiscount();

		return operator.evaluate(objectValue, this.getValue());
	}

	
}
