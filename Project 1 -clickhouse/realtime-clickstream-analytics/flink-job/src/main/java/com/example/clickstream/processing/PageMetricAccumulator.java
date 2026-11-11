package com.example.clickstream.processing;

import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * Window accumulator.
 *
 * <p>Distinct users and sessions are kept as map keys (a set) and the type information is declared
 * explicitly, so Flink serialises window state with its own POJO/Map serializers instead of falling
 * back to Kryo. Exact distinct counts are fine at page granularity; for very high-cardinality keys
 * swap these maps for a HyperLogLog sketch.
 */
@TypeInfo(PageMetricAccumulator.AccumulatorTypeInfoFactory.class)
public class PageMetricAccumulator {

    public long eventCount;
    public Map<String, Boolean> userIds = new HashMap<>();
    public Map<String, Boolean> sessionIds = new HashMap<>();
    public double revenue;

    public PageMetricAccumulator() {
    }

    /** Declares the accumulator as a POJO with Map-typed fields. */
    public static class AccumulatorTypeInfoFactory extends TypeInfoFactory<PageMetricAccumulator> {
        @Override
        public TypeInformation<PageMetricAccumulator> createTypeInfo(
                Type type, Map<String, TypeInformation<?>> genericParameters) {
            Map<String, TypeInformation<?>> fields = new HashMap<>();
            fields.put("eventCount", Types.LONG);
            fields.put("userIds", Types.MAP(Types.STRING, Types.BOOLEAN));
            fields.put("sessionIds", Types.MAP(Types.STRING, Types.BOOLEAN));
            fields.put("revenue", Types.DOUBLE);
            return Types.POJO(PageMetricAccumulator.class, fields);
        }
    }
}
