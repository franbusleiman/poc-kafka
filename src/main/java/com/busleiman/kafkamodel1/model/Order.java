package com.busleiman.kafkamodel1.model;


import lombok.*;


@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
@Getter
@Setter
public class Order {

    private String orderId;
    private String customerId;
    private String supplierId;
    private String firstName;
    private String lastName;
    private int items;
    private double price;
    private double weight;
    private boolean automatedEmail;
}
