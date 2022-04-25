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
		requiredDate : string
		identifier {
			orderId
		}
	}
	
	entity type ShipmentInfo{
		shipperId : int,
		companyName : string,
		phone : string,
		shippedDate : datetime,
		shipName : string,
		shipAddress : string,
		shipCity : string,
		shipRegion : string,
		shipPostalCode : string,
		shipCountry : string
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
		category[1] : Category
	}
	
	relationship type orders_details{
		ordered_product[0-N] : Products,
		order[0-N] : Orders,
		unitPrice : int,
		qty : int,
		discount : int
	}
	
	relationship type submit{
		orderedByCustomer[0-N] : Orders,
		customer[1] : Customers
	}
	
	relationship type accountableEmployee{
		orders[0-N] : Orders,
		accountableEmployee[1] : Employees
	}
	
	relationship type hierarchy{
		hasEmployees[0-N] : Employees,
		subordinateOf[0-1] : Employees
	}
	
	relationship type employeeTerritories{
		territories[0-N] : Territories,
		employee[0-N] : Employees
	}
	
	relationship type territoryRegions{
		territory[0-N] : Territories,
		regions[1] : Region
	}
	
	relationship type shipment{
		orderShipped[1] : Orders,
		shipper[0-1] : ShipmentInfo
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
	}
}

mapping rules{
	
}
