// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"fmt"
	"os"
	"runtime/debug"
	"sync"
	"testing"
	"time"
)

const (
	DEBUG  = -2
	TRACE  = -1
	INFO   = 0
	WARN   = 1
	ERROR  = 2
	FATAL  = 3
	ASSERT = 4
)

var LogFunction func(level int, logID string, message string)

var printLogHeader = false

func EnableLogHeader() {
	printLogHeader = true
}

var printStackTrace = false

func EnableStackTrace() {
	printStackTrace = true
}

var testingT *testing.T

func setTestingT(t *testing.T) {
	testingT = t
}

func getLevelName(level int) string {
	switch level {
	case DEBUG:
		return "DEBUG"
	case TRACE:
		return "TRACE"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	case FATAL:
		return "FATAL"
	case ASSERT:
		return "ASSERT"
	default:
		return fmt.Sprintf("[%d]", level)
	}
}

var loggingLevel int

func IsDebugging() bool {
	return loggingLevel <= DEBUG
}

func IsTracing() bool {
	return loggingLevel <= TRACE
}

func SetLoggingLevel(level int) {
	loggingLevel = level
}

func LOG_DEBUG(logID string, format string, v ...interface{}) {
	logf(DEBUG, logID, format, v...)
}

func LOG_TRACE(logID string, format string, v ...interface{}) {
	logf(TRACE, logID, format, v...)
}

func LOG_INFO(logID string, format string, v ...interface{}) {
	logf(INFO, logID, format, v...)
}

func LOG_WARN(logID string, format string, v ...interface{}) {
	logf(WARN, logID, format, v...)
}

func LOG_ERROR(logID string, format string, v ...interface{}) {
	logf(ERROR, logID, format, v...)
}

func LOG_FATAL(logID string, format string, v ...interface{}) {
	logf(FATAL, logID, format, v...)
}

func LOG_ASSERT(logID string, format string, v ...interface{}) {
	logf(ASSERT, logID, format, v...)
}

type Exception struct {
	Level   int
	LogID   string
	Message string
}

var logMutex sync.Mutex

func logf(level int, logID string, format string, v ...interface{}) {

	message := fmt.Sprintf(format, v...)

	if LogFunction != nil {
		LogFunction(level, logID, message)
		return
	}

	now := time.Now()

	// Uncomment this line to enable unbufferred logging for tests
	// fmt.Printf("%s %s %s %s\n", now.Format("2006-01-02 15:04:05.000"), getLevelName(level), logID, message)

	if testingT != nil {
		if level < WARN {
			if level >= loggingLevel {
				testingT.Logf("%s %s %s %s\n",
					now.Format("2006-01-02 15:04:05.000"), getLevelName(level), logID, message)
			}
		} else {
			testingT.Errorf("%s %s %s %s\n",
				now.Format("2006-01-02 15:04:05.000"), getLevelName(level), logID, message)
		}
	} else {
		logMutex.Lock()
		defer logMutex.Unlock()

		if level >= loggingLevel {
			if printLogHeader {
				fmt.Printf("%s %s %s %s\n",
					now.Format("2006-01-02 15:04:05.000"), getLevelName(level), logID, message)
			} else {
				fmt.Printf("%s\n", message)
			}
		}
	}

	if level > WARN {
		panic(Exception{
			Level:   level,
			LogID:   logID,
			Message: message,
		})
	}
}

const (
	duplicacyExitCode = 100
	otherExitCode     = 101
)

// This is the function to be called before exiting when an error occurs.
var RunAtError func() = func() {}

func CatchLogException() {
	if r := recover(); r != nil {
		switch e := r.(type) {
		case Exception:
			if printStackTrace {
				debug.PrintStack()
			}
			RunAtError()
			os.Exit(duplicacyExitCode)
		default:
			fmt.Fprintf(os.Stderr, "%v\n", e)
			debug.PrintStack()
			RunAtError()
			os.Exit(otherExitCode)
		}
	}
}
