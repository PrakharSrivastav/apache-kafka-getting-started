# Business case

The scenario we intend to create is as follows

- Central Invoice service sends out invoices to different customers.
- Customer asks their Bank to pay the invoice.
- Bank attempts to pay the invoice and notifies Invoice Service and Customer with status of invoice.

## Systems involved with their operations

- Invoice Service
    - createInvoice() - creates a random invoice and sends it to a topic.
    - settleInvoice() - reads from a topic and updates the status of an earlier invoice.
- Customer Service
    - findInvoice() - reads a topic to check for their invoice.
    - attemptInvoicePayment() - sends a message to a topic for bank to process it.
    - readNotification() - reads a topic to check the status of the payment attempt on invoice.
- Banking Service
    - attemptPayment() - reads a topic to find a new invoice to be paid.
    - notifyPaymentStatus() - sends a notification message after a payment attempt is made.
    
## Topics

- NEW_INVOICE : 
    - InvoiceService -> publishes a new invoice message for customer
    - CustomerService -> consumes a new invoice message.
- SETTLE_INVOICE : 
    - CustomerService -> publishes a message for bank to process invoice.
    - BankService -> consumes a message to attempt a payment
- NOTIFICATION:
    - BankService -> publishes a message status for the payment attempt
    - InvoiceService -> consumes message to update the status of the invoice
    - CustomerService -> consumes message to update the status of the invoice

## Sequence Diagram
Reference-style: 
![alt text][logo]

[logo]: ./images/sequence.png "Sequence diagram"

## Process Flow
![alt text][process]

[process]: ./images/process.png "Process flow"
