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
	enableAsync   = 0
	enableDebug   = 1
	enableConsole = 1
	enableFile    = 0
)

func Debug(server int, format string, info ...interface{}) {
	if enableDebug == 0 {
		return
	}

	if enableFile == 1 && file == nil {
		filePath := prefix + time.Now().String() + suffix
		file, _ = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	}

	str := fmt.Sprintf("%s [NODE %d]", time.Now().Format("15:04:05.000"), server)

	str += fmt.Sprintf(format, info...)

	str += "\n"

	switch enableAsync {
	case 1:
		go write(str)
	case 0:
		write(str)
	}

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