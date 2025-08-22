package service

import (
	"fmt"
	"os"
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
	FATAL    = iota
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

func getPrefixLogString(level int) string {
	switch level {
	case DEBUG:
		return fmt.Sprintf("%v :: [ \x1b[36mDEBUG\x1b[0m ] - ", time.Now().Format(time.UnixDate))
	case INFO:
		return fmt.Sprintf("%v :: [ \x1b[32mINFO\x1b[0m ] - ", time.Now().Format(time.UnixDate))
	case WARNING:
		return fmt.Sprintf("%v :: [ \x1b[33mWARNING\x1b[0m ] - ", time.Now().Format(time.UnixDate))
	case ERROR:
		return fmt.Sprintf("%v :: [ \x1b[31mERROR\x1b[0m ] - ", time.Now().Format(time.UnixDate))
	case FATAL:
		return fmt.Sprintf("%v :: [ \x1b[30m\x1b[90mFATAL\x1b[0m ] - ", time.Now().Format(time.UnixDate))
	}
	return ""
}

func Debug(fmtStr string, args ...any) {
	if logger.logLevel <= DEBUG {
		fmt.Printf(getPrefixLogString(DEBUG)+fmtStr+"\n", args...)
	}
}

func Info(fmtStr string, args ...any) {
	if logger.logLevel <= INFO {
		fmt.Printf(getPrefixLogString(INFO)+fmtStr+"\n", args...)
	}
}

func Warning(fmtStr string, args ...any) {
	if logger.logLevel <= WARNING {
		fmt.Printf(getPrefixLogString(WARNING)+fmtStr+"\n", args...)
	}
}

func Error(fmtStr string, args ...any) {
	if logger.logLevel <= ERROR {
		fmt.Printf(getPrefixLogString(ERROR)+fmtStr+"\n", args...)
	}
}

func Fatal(fmtStr string, args ...any) {
	if logger.logLevel <= FATAL {
		fmt.Printf(getPrefixLogString(FATAL)+fmtStr+"\n", args...)
		os.Exit(1)
	}
}
