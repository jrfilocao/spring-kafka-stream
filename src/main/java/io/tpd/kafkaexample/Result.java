package io.tpd.kafkaexample;

import java.util.Map;

public record Result(Map<String, Long> keyValues, int totalUniqueElements) {
}