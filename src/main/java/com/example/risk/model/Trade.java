package com.example.risk.model;


public record Trade(String tradeId, String accountId, double notional, Side side, long eventTs) {
}
