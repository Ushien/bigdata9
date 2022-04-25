conceptual schema conceptualSchema{

	entity type Products {
		productId : int,
		productName : string,
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
}

databases {
	
}

physical schemas {
	
}

mapping rules{
	
}