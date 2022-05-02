conceptual schema csmodel {
	

entity type Orders {
	id : int,
   OrderDate : date,
   RequiredDate : date,
   ShippedDate : date,
   Freight : float,
   ShipName : string,
   ShipAddress : string,
   ShipCity : string,
   ShipRegion : string,
   ShipPostalCode : string,
   ShipCountry : string
   identifier{
   	id
   }
}


entity type Products {
   productId : int,
   ProductName : string,
   QuantityPerUnit : string,
   UnitPrice : float,
   UnitsInStock : int,
   UnitsOnOrder : int,
   ReorderLevel : int,
   Discontinued : bool
   identifier{
   	productId
   }
}

entity type Suppliers {
   supplierId : int,
   CompanyName : string,
   ContactName : string,
   ContactTitle : string,
   Address : string,
   City : string,
   Region : string,
   PostalCode : string,
   Country : string,
   Phone : string,
   Fax : string,
   HomePage : text
   identifier{
   	supplierId
   }
}

entity type Customers {
   customerID : string,
   CompanyName : string,
   ContactName : string,
   ContactTitle : string,
   Address : string,
   City : string,
   Region : string,
   PostalCode : string,
   Country : string,
   Phone : string,
   Fax : string
   identifier {
   	customerID
   }
}

entity type Categories {
   categoryID : int,
   CategoryName : string,
   Description : text,
   Picture : blob
   identifier {
   	categoryID
   }
}

entity type Shippers {
   shipperID : int,
   CompanyName : string,
   Phone : string
   identifier{
   	shipperID
   }
}

entity type Employees {
   employeeID : int,
   LastName : string,
   FirstName : string,
   Title : string,
   TitleOfCourtesy : string,
   BirthDate : date,
   HireDate : date,
   Address : string,
   City : string,
   Region : string,
   PostalCode : string,
   Country : string,
   HomePhone : string,
   Extension : string,
   Photo : blob,
   Notes : text,
   PhotoPath : string,
   Salary : float
   identifier{
   	employeeID
   }
}


entity type Region {
   regionID : int,
   RegionDescription : string
   identifier{
   	regionID
   }
}

entity type Territories {
   territoryID : string,
   TerritoryDescription : string
   identifier {
   	territoryID
   }
}

relationship type locatedIn {
	territories[1] : Territories, 
	region[0-N] : Region
}

relationship type works {
	employed[0-N] : Employees,
	territories[0-N] : Territories 
}

relationship type reportsTo{
	subordonee[0-1] : Employees,
	boss[0-N] : Employees
}

relationship type supply {
	suppliedProduct[0-1] : Products,
	supplier[0-N] : Suppliers
}

relationship type typeOf {
	product[0-1] : Products,
	category[0-N] : Categories 
}

relationship type buy {
	boughtOrder[1] : Orders,
	customer[0-N] : Customers 
}

relationship type register {
	processedOrder[1] : Orders,
	employeeInCharge[0-N] : Employees 
}

relationship type ships {
	shippedOrder[1] : Orders,
	shipper[0-N] : Shippers
}

relationship type composedOf {
	order[0-N] : Orders,
	orderedProducts[0-N] : Products,
	UnitPrice : float,
	Quantity : int,
	Discount : float 
}
}
physical schemas {
	document schema mongoDB : myMongoDB{
		collection Orders{
			fields{
				OrderID,
			   OrderDate,
			   RequiredDate,
			   Freight,
			   ShipmentInfo [1]{
				   ShippedDate,
				   ShipperID,
				   CompanyName,
				   Phone,
				   ShipName,
				   ShipAddress,
				   ShipCity,
				   ShipRegion,
				   ShipPostalCode,
				   ShipCountry
			   },
			   CustomerRef,
			   EmployeeRef
			}
			references{
				bought : CustomerRef -> kvDB.Customers.custid
				encoded : EmployeeRef -> relDB.Employees.EmployeeID
			}				
			
		}
	}
	
	key value schema kvDB : myRedisDB{
		kvpairs Suppliers{
			key : "SUPPLIERS:"[SupplierID],
			value : hash {
			   CompanyName,
			   ContactName,
			   ContactTitle,
			   Address,
			   City,
			   Region,
			   PostalCode,
			   Country,
			   Phone,
			   Fax,
			   HomePage
			}
		}
		
		kvpairs Customers{
			key :  "CUSTOMER:"[custid],
			value : hash{
			   CompanyName,
			   ContactName,
			   ContactTitle,
			   Address,
			   City,
			   Region,
			   PostalCode,
			   Country,
			   Phone,
			   Fax
			}
		}
		
		kvpairs CustomersPurchased{
			key : "CUSTOMER:"[customerid]":ORDERS",
			value : list {
				orderref
			}
			references {
				purchases : orderref -> mongoDB.Orders.OrderID
			}
		}
	}
	
	relational schema relDB : myRelDB {

		table Products {
			columns{
			   ProductID,
			   ProductName,
			   QuantityPerUnit,
			   UnitPrice,
			   UnitsInStock,
			   UnitsOnOrder,
			   ReorderLevel,
			   Discontinued,
			   SupplierRef,
			   CategoryRef
			}
			references{
				supply : SupplierRef -> kvDB.Suppliers.SupplierID
				isCategory : CategoryRef -> Categories.CategoryID
			}
		}

		table Categories {
			columns{
			   CategoryID,
			   CategoryName,
			   Description,
			   Picture
			}
		}

		table Employees {
			columns{
			   EmployeeID,
			   LastName,
			   FirstName,
			   Title,
			   TitleOfCourtesy,
			   BirthDate,
			   HireDate,
			   HomePhone,
			   Extension,
			   Photo,
			   Notes,
			   PhotoPath,
			   Salary,
			   Address,
			   City,
			   Region,
			   PostalCode,
			   Country,
			   ReportsTo
			}
			references{
				manager : ReportsTo -> EmployeeID
			}
			
		}

		table Region {
			columns{
			   RegionID,
			   RegionDescription
			}
		}

		table Territories {
			columns{
	 	  		TerritoryID,
			   	TerritoryDescription,
			   	RegionRef
			}
			references{
				located : RegionRef -> Region.RegionID
			}
		}
		
		table Order_Details {
			columns{
				OrderRef,
				ProductRef,
				UnitPrice,
				Quantity,
				Discount
			}
			references{
				order : OrderRef -> mongoDB.Orders.OrderID
				purchasedProducts : ProductRef -> Products.ProductID
			}
		}
		
		table EmployeeTerritories {
			columns {
				EmployeeRef,
				TerritoryRef
			}
			references{
				employee : EmployeeRef -> Employees.EmployeeID
				territory : TerritoryRef -> Territories.TerritoryID
			}
		}
	}
}

mapping rules {
	csmodel.Orders(id, Freight, OrderDate,RequiredDate) -> mongoDB.Orders(OrderID,Freight,OrderDate,RequiredDate),
	csmodel.Orders(ShipAddress,ShipCity, ShipCountry, ShipName, ShippedDate, ShipPostalCode, ShipRegion) -> mongoDB.Orders.ShipmentInfo(ShipAddress,ShipCity, ShipCountry, ShipName, ShippedDate, ShipPostalCode, ShipRegion),
	csmodel.Products( productId,Discontinued,ProductName,QuantityPerUnit,ReorderLevel,UnitPrice,UnitsInStock,UnitsOnOrder) -> relDB.Products( ProductID,Discontinued,ProductName,QuantityPerUnit,ReorderLevel,UnitPrice,UnitsInStock,UnitsOnOrder),
	csmodel.Suppliers( supplierId,Address,City,CompanyName,ContactName,ContactTitle,Country,Fax,HomePage,Phone,PostalCode,Region) -> kvDB.Suppliers( SupplierID,Address,City,CompanyName,ContactName,ContactTitle,Country,Fax,HomePage,Phone,PostalCode,Region), 
	csmodel.Customers(customerID,Address,City,CompanyName,ContactName,ContactTitle,Country,Fax,Phone,PostalCode,Region) -> kvDB.Customers(custid, Address, City, CompanyName, ContactName,ContactTitle, Country,Fax,Phone, PostalCode, Region),
	csmodel.Customers( customerID) -> kvDB.CustomersPurchased( customerid),
	csmodel.Categories( categoryID,CategoryName,Description,Picture) -> relDB.Categories( CategoryID,CategoryName,Description,Picture),
	csmodel.Shippers( shipperID,CompanyName,Phone) -> mongoDB.Orders.ShipmentInfo(ShipperID,CompanyName,Phone),
	csmodel.Employees( employeeID,Address,BirthDate,City,Country,Extension,FirstName,LastName,HireDate,HomePhone,Notes,Photo,PhotoPath,PostalCode,Region,Salary,Title,TitleOfCourtesy)
		->
		relDB.Employees( EmployeeID,Address,BirthDate,City,Country,Extension,FirstName,LastName,HireDate,HomePhone,Notes,Photo,PhotoPath,PostalCode,Region,Salary,Title,TitleOfCourtesy),
	csmodel.reportsTo.subordonee -> relDB.Employees.manager,  
	csmodel.Region( regionID,RegionDescription) -> relDB.Region( RegionID,RegionDescription),
	csmodel.Territories( territoryID,TerritoryDescription) -> relDB.Territories( TerritoryID,TerritoryDescription),
	csmodel.supply.suppliedProduct -> relDB.Products.supply,
	csmodel.typeOf.product -> relDB.Products.isCategory,
	csmodel.locatedIn.territories -> relDB.Territories.located,
	csmodel.ships.shippedOrder -> mongoDB.Orders.ShipmentInfo(), 
	csmodel.register.processedOrder -> mongoDB.Orders.encoded,
	csmodel.buy.boughtOrder -> mongoDB.Orders.bought,
	csmodel.buy.customer -> kvDB.CustomersPurchased.purchases,
	rel : csmodel.composedOf( Discount,Quantity,UnitPrice) -> relDB.Order_Details( Discount,Quantity,UnitPrice),
	csmodel.composedOf.orderedProducts -> relDB.Order_Details.purchasedProducts,
	csmodel.composedOf.order -> relDB.Order_Details.order,
	csmodel.works.employed -> relDB.EmployeeTerritories.employee,
	csmodel.works.territories -> relDB.EmployeeTerritories.territory
	
}
databases {
	mysql myRelDB {
		dbname : "reldata"
		host : "localhost"
		port : 3399
		login : "root"
		password : "password"
	}
	
	mongodb myMongoDB{
		host : "localhost"
		port : 27777
	}
	
	redis myRedisDB{
		host : "localhost"
		port : 6666
	}
}