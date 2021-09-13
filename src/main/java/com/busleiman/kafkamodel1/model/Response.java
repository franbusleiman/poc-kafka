package com.busleiman.kafkamodel1.model;


import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
public class Response{

private String details;
private boolean processedCorrectly;
}
