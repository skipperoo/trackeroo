package logger

import (
	"fmt"
	"os"
	"strings"
	"time"
)

type Logger struct {
	logLevel int
	logChan  chan map[string]any
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

func newLogger(logLevel int) *Logger {
	return &Logger{
		logLevel: logLevel,
		logChan:  make(chan map[string]any, 1024),
	}
}

func (l *Logger) run() {
	for msg := range l.logChan {
		level, ok := msg["log_level"].(int)
		if !ok {
			continue
		}
		if level < l.logLevel {
			continue
		}
		message, ok := msg["message"].(string)
		if !ok {
			continue
		}
		fmt.Printf("%s %s", getPrefixLogString(level), message)
	}
}

func (l Logger) close() {
	close(l.logChan)
}

func InitLogger() {
	logLevel := strings.ToUpper(os.Getenv("LOG_LEVEL"))
	fmt.Println("######", logLevel)
	switch logLevel {
	case "DEBUG":
		logger = newLogger(DEBUG)
	case "INFO":
		logger = newLogger(INFO)
	case "WARNING":
		logger = newLogger(WARNING)
	case "ERROR":
		logger = newLogger(ERROR)
	case "FATAL":
		logger = newLogger(FATAL)
	case "DISABLED":
		logger = newLogger(DISABLED)
	default:
		logger = newLogger(INFO)
	}
	go logger.run()
}

func CloseLogger() {
	logger.close()
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
	logger.logChan <- map[string]any{
		"log_level": DEBUG,
		"message":   fmt.Sprintf(fmtStr+"\n", args...),
	}
	// if logger.logLevel <= DEBUG {
	// 	fmt.Printf(getPrefixLogString(DEBUG)+fmtStr+"\n", args...)
	// }
}

func Info(fmtStr string, args ...any) {
	logger.logChan <- map[string]any{
		"log_level": INFO,
		"message":   fmt.Sprintf(fmtStr+"\n", args...),
	}
	// if logger.logLevel <= INFO {
	// 	fmt.Printf(getPrefixLogString(INFO)+fmtStr+"\n", args...)
	// }
}

func Warning(fmtStr string, args ...any) {
	logger.logChan <- map[string]any{
		"log_level": WARNING,
		"message":   fmt.Sprintf(fmtStr+"\n", args...),
	}
	// if logger.logLevel <= WARNING {
	// 	fmt.Printf(getPrefixLogString(WARNING)+fmtStr+"\n", args...)
	// }
}

func Error(fmtStr string, args ...any) {
	logger.logChan <- map[string]any{
		"log_level": ERROR,
		"message":   fmt.Sprintf(fmtStr+"\n", args...),
	}
	// if logger.logLevel <= ERROR {
	// 	fmt.Printf(getPrefixLogString(ERROR)+fmtStr+"\n", args...)
	// }
}

func Fatal(fmtStr string, args ...any) {
	if logger.logLevel <= FATAL {
		fmt.Printf(getPrefixLogString(FATAL)+fmtStr+"\n", args...)
		os.Exit(1)
	}
}
