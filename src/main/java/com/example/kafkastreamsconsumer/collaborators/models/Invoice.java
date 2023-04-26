package com.example.kafkastreamsconsumer.collaborators.models;

import lombok.Data;

//@Builder(builderClassName = "builder")
@Data
public class Invoice {

    private Long id;
    private double netAmount;
    private double taxAmount;
    private double totalAmount;
    private String desc;
}

