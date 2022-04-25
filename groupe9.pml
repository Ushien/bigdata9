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

}

databases {
	
}

physical schemas {
	
}

mapping rules{
	
}