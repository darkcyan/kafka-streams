package com.example.risk.model;

import java.time.LocalDate;

public record Exposure(LocalDate date, double value) {
}
