package model;

import java.util.Objects;
import java.util.Random;
import java.util.UUID;

public final class Invoice {
    private String invoiceId;
    private String customerId;
    private Float amount;
    private String status;

    public String getInvoiceId() {
        return invoiceId;
    }

    public Invoice(final String invoiceId, final String customerId, final Float amount, final String status) {
        this.invoiceId = invoiceId;
        this.customerId = customerId;
        this.amount = amount;
        this.status = status;
    }

    public static Invoice newRandomInvoice() {
        return new Invoice(UUID.randomUUID().toString(), UUID.randomUUID().toString(), new Random().nextFloat(), "NEW");
    }

    @Override
    public String toString() {
        return "Invoice{" +
                "invoiceId='" + invoiceId + '\'' +
                ", customerId='" + customerId + '\'' +
                ", amount=" + amount +
                ", status='" + status + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Invoice invoice = (Invoice) o;
        return Objects.equals(invoiceId, invoice.invoiceId) &&
                Objects.equals(customerId, invoice.customerId) &&
                Objects.equals(amount, invoice.amount) &&
                Objects.equals(status, invoice.status);
    }

    @Override
    public int hashCode() {

        return Objects.hash(invoiceId, customerId, amount, status);
    }
}
