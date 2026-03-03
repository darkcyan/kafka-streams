package com.example.risk.model;

public record Decision(Trade trade, double previousExposure, double newExposure, double limit, boolean approved, String reason) {
}
