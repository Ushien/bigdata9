databases {
	
	mysql MySQL{
		host : "localhost"
		port : 3399
		dbname : "reldata"
		login : "root"
		password : "password"
	}
	
	redis Redis{
		host : "localhost"
		port : 6666
	}
	
	mongodb Mongo{
	host : "localhost"
	port : 27777
	dbname : "myMongoDB"
	}	
}

conceptual schema conceptualSchema{
	
	entity type Categories{
		categoryId : int,
		categoryName : string,
		description : string,
		picture : blob
		identifier {
			categoryId
		}
	}
	
	entity type Orders{
		orderId : int,
		freight : float,
		orderDate : datetime,
		requiredDate : string,
		shippedDate : datetime,
		shipName : string,
		shipAddress : string,
		shipCity : string,
		shipRegion : string,
		shipPostalCode : string,
		shipCountry : string
		identifier {
			orderId
		}
	}
	
	entity type Shipper{
		shipperId : int,
		companyName : string,
		phone : string
		identifier {
			shipperId
		}
	}

	entity type Products {
		productId : int,
		productName : string,
		supplierRef : int,
		categoryRef : int,
		quantityPerUnit : string,
		unitPrice : float,
		unitsInStock : int,
		unitsOnOrder : int,
		reorderLevel : int,
		discontinued : bool
		identifier {
			productId
		}
	}
	
	entity type Employee {
		employeeID : int,     
		lastName : string,      
		firstName :string,    
		title : int,        
		titleOfCourtesy : int,
		birthDate : datetime,
		hireDate : datetime,    
		address : string,        
		city : string,        
		region : string,         
		postalCode: string,    
		country : string,       
		homePhone : string,
		extension : string,     
		photo : blob,       
		notes : string,       
		reportsTo : int,      
		photoPath : string,      
		salary : float 
		identifier{
			employeeID
		}       
	}
	entity type Region{
		regionID : int,          
		regionDescription: string
		identifier{
			regionID
		}	
	}
	
	entity type Territories{
		territoryID : string,         
		territoryDescription : string
		identifier{
			territoryID
		}          
	}
	entity type Customers {
		customerId : string,
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
			customerId
		}
	}
	entity type Suppliers {
		supplierId : string,
		companyName : string,
		contactName : string,
		address : string,
		city : string,
		region : string,
		postalCode : string,
		country : string,
		phone : string,
		fax : string,
		homePage : string
		identifier {
			supplierId
		}	
	}
	relationship type supply{
		suppliedProduct[1] : Products,
		supplier[0-N] : Suppliers
	}
	
	relationship type concern{
		product[1] : Products,
		category[0-N] : Categories
	}
	
	relationship type orders_details{
		ordered_product[0-N] : Products,
		order[0-N] : Orders,
		unitPrice : float,
		qty : int,
		discount : int
	}
	
	relationship type submit{
		orderedByCustomer[1] : Orders,
		customer[0-N] : Customers
	}
	
	relationship type handle{
		orders[1] : Orders,
		accountableEmployee[0-N] : Employee
	}
	
	relationship type hierarchy{
		hasEmployees[0-N] : Employee,
		subordinateOf[0-1] : Employee
	}
	
	relationship type employeeTerritories{
		territories[0-N] : Territories,
		employee[0-N] : Employee
	}
	
	relationship type territoryRegions{
		territory[1] : Territories,
		regions[1-N] : Region
	}
	
	relationship type shipment{
		orderShipped[1] : Orders,
		shipper[0-1] : Shipper
	}
}


physical schemas {
	relational schema myRelSchema : MySQL{
		table Categories{
			columns{
				categoryId,
				categoryName,
				description,
				picture
			}
		}
		
		table Employees{
			columns{
				employeeID,     
				lastName,      
				firstName,   
				title,        
				titleOfCourtesy,
				birthDate,
				hireDate,    
				address,        
				city,        
				region,         
				postalCode,    
				country,       
				homePhone,
				extension,     
				photo,       
				notes,       
				reportsTo,      
				photoPath,      
				salary
			}
			references{
				reportsToFK: reportsTo -> Employees.employeeID
			}
		}
		
		table Territories{
			columns{
				TerritoryID,
				TerritoryDescription,
				RegionRef
			}
			references{
				RegionRefFK : RegionRef -> Region.regionId
			}
		}
		
		table EmployeeTerritories{
			columns{
				EmployeeRef,
				TerritoryRef
			}
			references{
				EmployeeRefFK : EmployeeRef -> Employees.employeeID
				TerritoryRefFK : TerritoryRef -> Territories.TerritoryID
			}
		}
		
		table Region{
			columns{
				regionId,
				regionDescription
			}
		}
		
		table Products{
			columns{
				ProductID,
				ProductName,
				SupplierRef,
				CategoryRef,
				QuantityPerUnit,
				UnitPrice,
				UnitIsInStock,
				UnitsOnOrder,
				ReorderLevel,
				Discountinued
			}	
			references{
				SupplierRef: SupplierRef -> myRedisSchema.SuppliersKV.id
				CategoryRef: CategoryRef -> Categories.categoryId
			
		    }
		
	   }
		
		table Order_Details{
			columns{
				OrderRef,
				ProductRef,
				UnitPrice,
				Quantity,
				Discount
			}
			references {
				OrderRef: OrderRef -> mongoSchema.ordersCol.orderId
				ProductRef: ProductRef-> Products.ProductID 
				}
		}	
		
	}
	
	document schema mongoSchema: Mongo{
		
		collection ordersCol{
			fields {
				orderId,
				customerRef,
				employeeRef,
				freight,
				orderDate,
				requiredDate,
				shipperId,
				shippedDate,
				shipName,
				shipAddress,
				shipCity,
				shipRegion,
				shipPostalCode,
				shipCountry
			}
			
			references{
				shipper : shipperId -> mongoSchema.shipperCol.shipperId
			}
		}
		
		collection shipperCol{
			fields {
				shipperId,
				companyName,
				phone
			}
		}
		
	}
				
	key value schema myRedisSchema : Redis{
		kvpairs SuppliersKV{
			key : "SUPPLIER:"[id],
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
				Fax,
				HomePage
			}
		}
	}
	
	key value schema kv : Redis	{
		kvpairs customerPairs {
			key: "CUSTOMER:" [customerId],
			value : hash{
				companyName,
				contactName,
				contactTitle,
				address,
				city,
				region,
				postalCode,
				country,
				phone,
				fax				
			}
		}
		kvpairs customerPairs2 {
			key: "CUSTOMER:" [customerId] ":ORDERS",
			value : list{
				orderId
			}
			references{
				orders : orderId -> mongoSchema.ordersCol.orderId
			}
		}
	}
}

mapping rules{
	
	/* Products*/
	conceptualSchema.Products(productId, productName, quantityPerUnit, unitPrice, unitsInStock, unitsOnOrder, reorderLevel, discontinued)
	 -> myRelSchema.Products(ProductID, ProductName, QuantityPerUnit, UnitPrice, UnitIsInStock, UnitsOnOrder, ReorderLevel, Discountinued),
	 
	 /*concern association*/
	 conceptualSchema.concern.product -> myRelSchema.Products.CategoryRef,
	 
	 
	/* Order Detail (association -> Table)*/
	
	conceptualSchema.orders_details.ordered_product -> myRelSchema.Order_Details.OrderRef,
	
	conceptualSchema.orders_details.order -> myRelSchema.Order_Details.ProductRef,
	
	rel : conceptualSchema.orders_details( unitPrice, qty, discount) -> myRelSchema.Order_Details( UnitPrice, Quantity, Discount),
	

	/*Customers */

	conceptualSchema.Customers(customerId) -> kv.customerPairs(customerId),
	
	conceptualSchema.Customers(companyName, contactName, contactTitle, address, city, region, postalCode, country, phone, fax) ->
	kv.customerPairs(companyName, contactName, contactTitle, address, city, region, postalCode, country, phone, fax),
	
	/*Orders */
	conceptualSchema.Orders(orderId,freight,orderDate,requiredDate,shippedDate,shipName,shipAddress,shipCity,shipRegion,shipPostalCode,shipCountry)
	-> mongoSchema.ordersCol(orderId,freight,orderDate,requiredDate,shippedDate,shipName,shipAddress,shipCity,shipRegion,shipPostalCode,shipCountry),
	
	/*Shipper */
	conceptualSchema.Shipper(shipperId,companyName,phone)
	-> mongoSchema.shipperCol(shipperId,companyName,phone),
	
	/*Region */
	conceptualSchema.Region(regionID,regionDescription)
	-> myRelSchema.Region(regionId,regionDescription),
	
	/*relation supply */
	conceptualSchema.supply.suppliedProduct -> myRelSchema.Products.SupplierRef,
	
	/*relation submit */
	conceptualSchema.submit.orderedByCustomer -> myRedisSchema.customerPairs2.orders,
	
	/*relation hierarchy */
	conceptualSchema.hierarchy.subordinateOf -> myRelSchema.Employees.reportsToFK,
	
	/*relation employeeTerritories */
	conceptualSchema.employeeTerritories.employee -> myRelSchema.EmployeeTerritories.EmployeeRefFK,
	
	conceptualSchema.employeeTerritories.territories -> myRelSchema.EmployeeTerritories.TerritoryRefFK,
	
	/*relation shipment */
	conceptualSchema.shipment.shipper -> mongoSchema.ordersCol.shipper,
	
	/*handle*/
	conceptualSchema.handle.accountableEmployee -> mongoSchema.ordersCol.employeeRef,
	
	/*Employee*/
	conceptualSchema.Employee (employeeID, lastName, firstName, title, titleOfCourtesy, birthDate, hireDate, address, city, region, postalCode, country,homePhone,
				extension, photo, notes, reportsTo, photoPath, salary) -> myRelSchema.Employee( employeeID, lastName, firstName, title, titleOfCourtesy, birthDate, hireDate, address, city, region, postalCode, country,homePhone,
				extension, photo, notes, reportsTo, photoPath, salary),
				
	/*Territories*/
	conceptualSchema.Territories(TerritoryID, TerritoryDescription) -> myRelSchema.Territories(TerritoryID, TerritoryDescription),
	
	/* OrderDetail */
	conceptualSchema.OrderDetail(orderRef,productRef,unitPrice,quantity,discount)
	-> Tables_ph.OrderDetails(orderRef,productRef,unitPrice,quantity,discount),
	
	/* TerritoryRegions */
	conceptualSchema.territoryRegions.territory
	-> myRelSchema.Territories.RegionRefFK
	
}

