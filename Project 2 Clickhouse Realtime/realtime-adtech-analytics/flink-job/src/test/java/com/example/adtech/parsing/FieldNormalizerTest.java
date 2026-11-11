package com.example.adtech.parsing;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FieldNormalizerTest {

    @ParameterizedTest
    @CsvSource({"de, DE", "Germany, ZZ", "' us ', US"})
    void country(String input, String expected) {
        assertEquals(expected, FieldNormalizer.country(input));
    }

    @ParameterizedTest
    @CsvSource({"Mobile, mobile", "CTV, ctv", "smartwatch, unknown"})
    void deviceType(String input, String expected) {
        assertEquals(expected, FieldNormalizer.deviceType(input));
    }

    @ParameterizedTest
    @CsvSource({"https://www.Site.com/path?q=1, site.com", "news1.example.com, news1.example.com", "'', unknown"})
    void domain(String input, String expected) {
        assertEquals(expected, FieldNormalizer.domain(input));
    }

    @ParameterizedTest
    @CsvSource({"VIDEO, video", "interstitial, other"})
    void adFormat(String input, String expected) {
        assertEquals(expected, FieldNormalizer.adFormat(input));
    }
}
