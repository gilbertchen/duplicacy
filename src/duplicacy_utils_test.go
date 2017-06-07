// Copyright (c) Acrosync LLC. All rights reserved.
// Licensed under the Fair Source License 0.9 (https://fair.io/)
// User Limitation: 5 users

package duplicacy

import (
    "io"
    "io/ioutil"
    "time"
    "bytes"

    crypto_rand "crypto/rand"

    "testing"

)

func TestMatchPattern(t *testing.T) {

    // Test cases were copied from Matching Wildcards: An Empirical Way to Tame an Algorithm
    // By Kirk J. Krauss, October 07, 2014

    DATA := [] struct {
        text string
        pattern string
        matched bool
    } {
        // Cases with repeating character sequences.
        { "abcccd", "*ccd", true },
        { "mississipissippi", "*issip*ss*", true },
        { "xxxx*zzzzzzzzy*f", "xxxx*zzy*fffff", false },
        { "xxxx*zzzzzzzzy*f", "xxx*zzy*f", true },
        { "xxxxzzzzzzzzyf", "xxxx*zzy*fffff", false },
        { "xxxxzzzzzzzzyf", "xxxx*zzy*f", true },
        { "xyxyxyzyxyz", "xy*z*xyz", true },
        { "mississippi", "*sip*", true },
        { "xyxyxyxyz", "xy*xyz", true },
        { "mississippi", "mi*sip*", true },
        { "ababac", "*abac*", true },
        { "ababac", "*abac*", true },
        { "aaazz", "a*zz*", true },
        { "a12b12", "*12*23", false },
        { "a12b12", "a12b", false },
        { "a12b12", "*12*12*", true },

        // More double wildcard scenarios.
        { "XYXYXYZYXYz", "XY*Z*XYz", true },
        { "missisSIPpi", "*SIP*", true },
        { "mississipPI", "*issip*PI", true },
        { "xyxyxyxyz", "xy*xyz", true },
        { "miSsissippi", "mi*sip*", true },
        { "miSsissippi", "mi*Sip*", false },
        { "abAbac", "*Abac*", true },
        { "abAbac", "*Abac*", true },
        { "aAazz", "a*zz*", true },
        { "A12b12", "*12*23", false },
        { "a12B12", "*12*12*", true },
        { "oWn", "*oWn*", true },

        // Completely tame (no wildcards) cases.
        { "bLah", "bLah", true },
        { "bLah", "bLaH", false },

        // Simple mixed wildcard tests suggested by IBMer Marlin Deckert.
        { "a", "*?", true },
        { "ab", "*?", true },
        { "abc", "*?", true },

        // More mixed wildcard tests including coverage for false positives.
        { "a", "??", false },
        { "ab", "?*?", true },
        { "ab", "*?*?*", true },
        { "abc", "?*?*?", true },
        { "abc", "?*?*&?", false },
        { "abcd", "?b*??", true },
        { "abcd", "?a*??", false },
        { "abcd", "?*?c?", true },
        { "abcd", "?*?d?", false },
        { "abcde", "?*b*?*d*?", true },

        // Single-character-match cases.
        { "bLah", "bL?h", true },
        { "bLaaa", "bLa?", false },
        { "bLah", "bLa?", true },
        { "bLaH", "?Lah", false },
        { "bLaH", "?LaH", true },
    }

    for _, data := range DATA {
        if matchPattern(data.text, data.pattern) != data.matched {
            t.Errorf("text: %s, pattern %s, expected: %t", data.text, data.pattern, data.matched)
        }
    }

}

func TestRateLimit(t *testing.T) {
    content := make([]byte, 100 * 1024)
    _, err := crypto_rand.Read(content)
    if err != nil {
        t.Errorf("Error generating random content: %v", err)
        return
    }

    expectedRate := 10
    rateLimiter := CreateRateLimitedReader(content, expectedRate)

    startTime := time.Now()
    n, err := io.Copy(ioutil.Discard, rateLimiter)
    if err != nil {
        t.Errorf("Error reading from the rate limited reader: %v", err)
        return
    }
    if int(n) != len(content) {
        t.Errorf("Wrote %s bytes instead of %s", n, len(content))
        return
    }

    elapsed := time.Since(startTime)
    actualRate := float64(len(content)) / elapsed.Seconds() / 1024
    t.Logf("Elapsed time: %s, actual rate: %.3f kB/s, expected rate: %d kB/s", elapsed, actualRate, expectedRate)

    startTime = time.Now()
    n, err = RateLimitedCopy(ioutil.Discard, bytes.NewBuffer(content), expectedRate)
    if err != nil {
        t.Errorf("Error writing with rate limit: %v", err)
        return
    }
    if int(n) != len(content) {
        t.Errorf("Copied %s bytes instead of %s", n, len(content))
        return
    }

    elapsed = time.Since(startTime)
    actualRate = float64(len(content)) / elapsed.Seconds() / 1024
    t.Logf("Elapsed time: %s, actual rate: %.3f kB/s, expected rate: %d kB/s", elapsed, actualRate, expectedRate)

}
