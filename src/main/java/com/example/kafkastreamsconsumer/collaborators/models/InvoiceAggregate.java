package com.example.kafkastreamsconsumer.collaborators.models;

import lombok.Data;

@Data
public class InvoiceAggregate {
    private Long id;
    private double netAmount;
    private double taxAmount;
    private double totalAmount;
    private String desc;
}

