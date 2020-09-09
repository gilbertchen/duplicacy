// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"fmt"
	"os"
	"log"
	"runtime/debug"
	"sync"
	"testing"
	"time"
	"regexp"
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

// Contains the ids of logs that won't be displayed
var suppressedLogs map[string]bool = map[string]bool{}

func SuppressLog(id string) {
	suppressedLogs[id] = true
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

// log function type for passing as a parameter to functions
type LogFunc func(logID string, format string, v ...interface{})

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
		if level <= WARN {
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
			if level <= ERROR && len(suppressedLogs) > 0 {
				if _, found := suppressedLogs[logID]; found {
					return
				}
			}

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

// Set up logging for libraries that Duplicacy depends on.  They can call 'log.Printf("[ID] message")'
// to produce logs in Duplicacy's format
type Logger struct {
	formatRegex *regexp.Regexp
}

func (logger *Logger) Write(line []byte) (n int, err error) {
	n = len(line)
	for len(line) > 0 && line[len(line) - 1] == '\n' {
		line = line[:len(line) - 1]
	}
	matched := logger.formatRegex.FindStringSubmatch(string(line))
	if matched != nil {
		LOG_INFO(matched[1], "%s", matched[2])
	} else {
		LOG_INFO("LOG_DEFAULT", "%s", line)
	}

    return
}

func init() {
	log.SetFlags(0)
	log.SetOutput(&Logger{ formatRegex: regexp.MustCompile(`^\[(.+)\]\s*(.+)`) })
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
