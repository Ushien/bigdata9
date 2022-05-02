conceptual schema csmodel {
	

entity type Orders {
	id : int,
   orderDate : date,
   requiredDate : date,
   shippedDate : date,
   freight : float,
   shipName : string,
   shipAddress : string,
   shipCity : string,
   shipRegion : string,
   shipPostalCode : string,
   shipCountry : string
   identifier{
   	id
   }
}


entity type Products {
   productId : int,
   productName : string,
   quantityPerUnit : string,
   unitPrice : float,
   unitsInStock : int,
   unitsOnOrder : int,
   reorderLevel : int,
   discontinued : bool
   identifier{
   	productId
   }
}

entity type Suppliers {
   supplierId : int,
   companyName : string,
   contactName : string,
   contactTitle : string,
   address : string,
   city : string,
   region : string,
   postalCode : string,
   country : string,
   phone : string,
   fax : string,
   homePage : text
   identifier{
   	supplierId
   }
}

entity type Customers {
   customerID : string,
   companyName : string,
   contactName : string,
   contactTitle : string,
   address : string,
   city : string,
   region : string,
   postalCode : string,
   country : string,
   phone : string,
   fax : string
   identifier {
   	customerID
   }
}

entity type Categories {
   categoryID : int,
   categoryName : string,
   description : text,
   picture : blob
   identifier {
   	categoryID
   }
}

entity type Shippers {
   shipperID : int,
   companyName : string,
   phone : string
   identifier{
   	shipperID
   }
}

entity type Employees {
   employeeID : int,
   lastName : string,
   firstName : string,
   title : string,
   titleOfCourtesy : string,
   birthDate : date,
   hireDate : date,
   address : string,
   city : string,
   region : string,
   postalCode : string,
   country : string,
   homePhone : string,
   extension : string,
   photo : blob,
   notes : text,
   photoPath : string,
   salary : float
   identifier{
   	employeeID
   }
}


entity type Region {
   regionID : int,
   regionDescription : string
   identifier{
   	regionID
   }
}

entity type Territories {
   territoryID : string,
   territoryDescription : string
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
	unitPrice : float,
	quantity : int,
	discount : float 
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
	csmodel.Orders(id, freight, orderDate,requiredDate) -> mongoDB.Orders(OrderID,Freight,OrderDate,RequiredDate),
	csmodel.Orders(shipAddress,shipCity, shipCountry, shipName, shippedDate, shipPostalCode, shipRegion) -> mongoDB.Orders.ShipmentInfo(ShipAddress,ShipCity, ShipCountry, ShipName, ShippedDate, ShipPostalCode, ShipRegion),
	csmodel.Products( productId,discontinued,productName,quantityPerUnit,reorderLevel,unitPrice,unitsInStock,unitsOnOrder) -> relDB.Products( ProductID,Discontinued,ProductName,QuantityPerUnit,ReorderLevel,UnitPrice,UnitsInStock,UnitsOnOrder),
	csmodel.Suppliers( supplierId,address,city,companyName,contactName,contactTitle,country,fax,homePage,phone,postalCode,region) -> kvDB.Suppliers( SupplierID,Address,City,CompanyName,ContactName,ContactTitle,Country,Fax,HomePage,Phone,PostalCode,Region), 
	csmodel.Customers(customerID,address,city,companyName,contactName,contactTitle,country,fax,phone,postalCode,region) -> kvDB.Customers(custid, Address, City, CompanyName, ContactName,ContactTitle, Country,Fax,Phone, PostalCode, Region),
	csmodel.Customers( customerID) -> kvDB.CustomersPurchased( customerid),
	csmodel.Categories( categoryID,categoryName,description,picture) -> relDB.Categories( CategoryID,CategoryName,Description,Picture),
	csmodel.Shippers( shipperID,companyName,phone) -> mongoDB.Orders.ShipmentInfo(ShipperID,CompanyName,Phone),
	csmodel.Employees( employeeID,address,birthDate,city,country,extension,firstName,lastName,hireDate,homePhone,notes,photo,photoPath,postalCode,region,salary,title,titleOfCourtesy)
		->
		relDB.Employees( EmployeeID,Address,BirthDate,City,Country,Extension,FirstName,LastName,HireDate,HomePhone,Notes,Photo,PhotoPath,PostalCode,Region,Salary,Title,TitleOfCourtesy),
	csmodel.reportsTo.subordonee -> relDB.Employees.manager,
	csmodel.Region( regionID,regionDescription) -> relDB.Region( RegionID,RegionDescription),
	csmodel.Territories( territoryID,territoryDescription) -> relDB.Territories( TerritoryID,TerritoryDescription),
	csmodel.supply.suppliedProduct -> relDB.Products.supply,
	csmodel.typeOf.product -> relDB.Products.isCategory,
	csmodel.locatedIn.territories -> relDB.Territories.located,
	csmodel.ships.shippedOrder -> mongoDB.Orders.ShipmentInfo(), 
	csmodel.register.processedOrder -> mongoDB.Orders.encoded,
	csmodel.buy.boughtOrder -> mongoDB.Orders.bought,
	csmodel.buy.customer -> kvDB.CustomersPurchased.purchases,
	rel : csmodel.composedOf( discount,quantity,unitPrice) -> relDB.Order_Details( Discount,Quantity,UnitPrice),
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