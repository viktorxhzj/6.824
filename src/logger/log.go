package logger

import (
	"fmt"
	"os"
	"time"
)

var (
	file          *os.File
	prefix        = "../logs/"
	suffix        = ".logger"
	INFO          = "[Debug] "
	ERROR         = "[Error] "
	enableDebug   = 1
	enableConsole = 1
	enableFile    = 1
)

func init() {
	if enableDebug == 1 && enableFile == 1 {
		filePath := prefix + time.Now().Format("15:04:05.000") + suffix
		file, _ = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	}
}

func Debug(server int, format string, info ...interface{}) {
	if enableDebug == 0 {
		return
	}

	str := fmt.Sprintf("%s [NODE %d]", time.Now().Format("15:04:05.000"), server)

	str += fmt.Sprintf(format, info...)

	str += "\n"

	write(str)
}

func write(str string) {
	if enableFile == 1 {
		_, err := file.WriteString(INFO + str)
		if err != nil {
			print(ERROR + "writing to file")
			return
		}
	}

	if enableConsole == 1 {
		print(INFO + str)
	}
}
