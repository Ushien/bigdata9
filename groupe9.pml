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
	
	entity type Employee{
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
		coutry : string,
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
		suppliedProduct[0-N] : Products,
		supplier[1] : Suppliers
	}
	
	relationship type concern{
		product[0-N] : Products,
		category[1] : Categories
	}
	
	relationship type orders_details{
		ordered_product[0-N] : Products,
		order[0-N] : Orders,
		unitPrice : float,
		qty : int,
		discount : int
	}
	
	relationship type submit{
		orderedByCustomer[0-N] : Orders,
		customer[1] : Customers
	}
	
	relationship type accountableEmployee{
		orders[0-N] : Orders,
		accountableEmployee[1] : Employee
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
		territory[0-N] : Territories,
		regions[1] : Region
	}
	
	relationship type shipment{
		orderShipped[1] : Orders,
		shipper[0-1] : Shipper
	}
}

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
		}
		
		table Territories{
			columns{
				TerritoryID,
				TerritoryDescription	
			}
			references{
				RegionRef : RegionRef -> Region.RegionID
			}
		}
		
		table EmployeeTerritories{
			columns{
				EmployeeRef,
				TerritoryRef
			}
			references{
				EmployeeRef : EmployeeRef -> Employees.employeeId
				TerritoryRef : TerritoryRef -> Territories.TerritoryId
			}
		}
		
		table Products{
			columns{
				ProductID,
				ProductName,
				\\SupplierRef,
				\\CategoryRef,
				QuantityPerUnit,
				UnitPrice,
				UnitIsInStock,
				UnitsOnOrder,
				ReorderLevel,
				Discountinued
			}	
			//references{SupplierRef: SupplierRef -> myRedisSchema.key,
			//CategoryRef: CategoryRef -> \\mettre nom schema.Categories.categoryId
			
		}
		
		table Order_Details{
			columns{
				//OrderRef,
				//ProductRef,
				UnitPrice,
				Quantity,
				Discount
			}
			//references {OrderRef: Order.OrderID -> myDocSchema.\\mettre nom schema._id,
			//ProductRef: ProductRef-> myRelSchema.Products.ProductID }
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
	conceptualSchema.Products(productId, productName, quantityPerUnit, unitPrice,
	 unitsInStock, unitsOnOrder, reorderLevel, discontinued)
	 -> myRelSchema.Products(ProductID, ProductName, QuantityPerUnit, UnitPrice, UnitIsInStock, UnitsOnOrder, ReorderLevel, Discountinued),
	 conceptualSchema.orders_details(ordered_product, order, UnitPrice, qty, discount) -> myRelSchema.Order_Details(OrderRef,
	ProductRef, UnitPrice, Quantity, Discount)
}
