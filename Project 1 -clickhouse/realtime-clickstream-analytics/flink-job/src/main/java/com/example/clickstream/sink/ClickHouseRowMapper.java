package com.example.clickstream.sink;

import java.io.IOException;
import java.io.Serializable;

/** Converts a stream element into one JSONEachRow line for ClickHouse. */
@FunctionalInterface
public interface ClickHouseRowMapper<T> extends Serializable {

    String toJsonRow(T value) throws IOException;
}
