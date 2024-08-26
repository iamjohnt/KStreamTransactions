package org.example;

public class Transaction {

    private String uid;
    private Double transactionAmount;

    public Transaction(String uid, Double transactionAmount) {
        this.uid = uid;
        this.transactionAmount = transactionAmount;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public Double getTransactionAmount() {
        return transactionAmount;
    }

    public void setTransactionAmount(Double transactionAmount) {
        this.transactionAmount = transactionAmount;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "uid='" + uid + '\'' +
                ", transactionAmount=" + transactionAmount +
                '}';
    }
}
