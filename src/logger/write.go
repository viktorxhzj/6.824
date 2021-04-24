package logger

import (
	"os"
	"time"
)

var file *os.File

func EnableLogger() {
	if file == nil {
		curPath, _ := os.Getwd()
		filePath := curPath + "/logs/" + time.Now().Format("2006-01-02") + "." + time.Now().Format("15:04") + ".logger"
		file, _ = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	}
}

func Write(str string) {
	_, err := file.WriteString(str)
	if err != nil {
		panic("failed to log")
	}
}
