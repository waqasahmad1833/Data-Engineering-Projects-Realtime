package com.example.adtech.sink;

import java.io.IOException;
import java.io.Serializable;

/** Converts a stream element into one JSONEachRow line. */
@FunctionalInterface
public interface ClickHouseRowMapper<T> extends Serializable {

    String toJsonRow(T value) throws IOException;
}
