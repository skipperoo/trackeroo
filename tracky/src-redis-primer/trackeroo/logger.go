package trackeroo

import (
	"fmt"
	"path/filepath"
	"runtime"
	"time"
)

type Logger struct {
	logLevel int
	logFile  string
}

const (
	DEBUG    = iota
	INFO     = iota
	WARNING  = iota
	ERROR    = iota
	DISABLED = iota
)

var logger *Logger

func newLogger(logLevel int, logFile string) *Logger {
	return &Logger{
		logLevel: logLevel,
		logFile:  logFile,
	}
}

func InitLogger(logLevel int, logFile string) {
	logger = newLogger(logLevel, logFile)
}

func GetLogPrefixString(level int) string {
	_, file, no, _ := runtime.Caller(2)
	file = filepath.Base(file)
	switch level {
	case DEBUG:
		return fmt.Sprintf("%v :: %s:%d :: [ \x1b[36mDEBUG\x1b[0m ] - ", time.Now().Format(time.UnixDate), file, no)
	case INFO:
		return fmt.Sprintf("%v :: [ \x1b[32mINFO\x1b[0m ] - ", time.Now().Format(time.UnixDate))
	case WARNING:
		return fmt.Sprintf("%v :: %s:%d :: [ \x1b[33mWARNING\x1b[0m ] - ", time.Now().Format(time.UnixDate), file, no)
	case ERROR:
		return fmt.Sprintf("%v :: %s:%d :: [ \x1b[31mERROR\x1b[0m ] - ", time.Now().Format(time.UnixDate), file, no)
	}
	return ""
}

func Debug(fmtStr string, args ...any) {
	if logger.logLevel <= DEBUG {
		fmt.Printf(GetLogPrefixString(DEBUG)+fmtStr+"\n", args...)
	}
}

func Info(fmtStr string, args ...any) {
	if logger.logLevel <= INFO {
		fmt.Printf(GetLogPrefixString(INFO)+fmtStr+"\n", args...)
	}
}

func Warning(fmtStr string, args ...any) {
	if logger.logLevel <= WARNING {
		fmt.Printf(GetLogPrefixString(WARNING)+fmtStr+"\n", args...)
	}
}

func Error(fmtStr string, args ...any) {
	if logger.logLevel <= ERROR {
		fmt.Printf(GetLogPrefixString(ERROR)+fmtStr+"\n", args...)
	}
}
