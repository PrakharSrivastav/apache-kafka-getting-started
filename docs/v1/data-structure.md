NEW_INVOICE : 
    InvoiceService --> CustomerService
    
    Headers
        Source: InvoiceService
        Target: [CustomerService]
        Content-Type : application/json
        Encoding: utf-8
    Body
        InvoiceId: String
        CustomerId: String
        Amount: Float
        Status: String

SETTLE_INVOICE : 
    CustomerService --> BankService
    
    Headers
        Source: CustomerService
        Target: [BankService]
        Content-Type: application/json
        Encoding: utf-8
    Body
        InvoiceId: String
        CustomerId: String
        AccountNumber: String
        Amount: Float
        Action: String
NOTIFICATION:
    BankService --> CustomerService, InvocieService
    
    Headers
        Source: BankService
        Target: [CustomerService , InvoiceService]
        Content-Type: application/json
        Encoding: utf-8
    Body
        InvoiceId: String
        CustomerId: String
        Amount: Float
        Status: String
        
    

    
        
            
    