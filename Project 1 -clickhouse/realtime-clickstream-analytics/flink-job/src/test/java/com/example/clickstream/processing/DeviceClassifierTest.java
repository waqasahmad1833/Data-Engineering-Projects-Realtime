package com.example.clickstream.processing;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DeviceClassifierTest {

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0 Safari/537.36 | desktop",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) Mobile/15E148 | mobile",
            "Mozilla/5.0 (Linux; Android 14; Pixel 8) Chrome/124.0 Mobile Safari/537.36 | mobile",
            "Mozilla/5.0 (iPad; CPU OS 17_4 like Mac OS X) Mobile/15E148 | tablet",
            "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html) | bot",
    })
    void classifiesKnownUserAgents(String userAgent, String expected) {
        assertEquals(expected, DeviceClassifier.classify(userAgent));
    }

    @ParameterizedTest
    @NullAndEmptySource
    void unknownWhenMissing(String userAgent) {
        assertEquals(DeviceClassifier.UNKNOWN, DeviceClassifier.classify(userAgent));
    }
}
