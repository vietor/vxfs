package glog

import (
	"fmt"
	"log"
	"os"
)

const (
	debugLog = iota
	infoLog
	warningLog
	errorLog
	fatalLog
)

var (
	logLevel     = debugLog
	logLevelName = []string{
		debugLog:   "DEBUG",
		infoLog:    "INFO",
		warningLog: "WARN",
		errorLog:   "ERROR",
		fatalLog:   "FATAL",
	}
)

func logPrintln(level int, args ...interface{}) {
	if level >= logLevel {
		log.Println("[" + logLevelName[level] + "] " + fmt.Sprint(args...))
	}
}

func logPrintf(level int, format string, args ...interface{}) {
	if level >= logLevel {
		log.Print("[" + logLevelName[level] + "] " + fmt.Sprintf(format, args...))
	}
}

func Debugln(args ...interface{}) {
	logPrintln(debugLog, args...)
}

func Debugf(format string, args ...interface{}) {
	logPrintf(debugLog, format, args...)
}

func Infoln(args ...interface{}) {
	logPrintln(infoLog, args...)
}

func Infof(format string, args ...interface{}) {
	logPrintf(infoLog, format, args...)
}

func Warningln(args ...interface{}) {
	logPrintln(warningLog, args...)
}

func Warningf(format string, args ...interface{}) {
	logPrintf(warningLog, format, args...)
}

func Errorln(args ...interface{}) {
	logPrintln(errorLog, args...)
}

func Errorf(format string, args ...interface{}) {
	logPrintf(errorLog, format, args...)
}

func Exitln(args ...interface{}) {
	logPrintln(fatalLog, args...)
	os.Exit(1)
}

func Exitf(format string, args ...interface{}) {
	logPrintf(fatalLog, format, args...)
	os.Exit(1)
}
