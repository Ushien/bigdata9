conceptual schema conceptualSchema{

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

}

databases {
	
}

physical schemas {
	
}

mapping rules{
	
}