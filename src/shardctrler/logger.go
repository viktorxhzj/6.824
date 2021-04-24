package shardctrler

import (
	"fmt"
	"time"

	"6.824/logger"
)

const (
	LOG_INFO  = "[INFO]"
	LOG_ERROR = "[ERROR]"
	LOG_DEBUG = "[DEBUG]"
)

const (
	CLI_FORMAT = "[CTRL-CLI %d] "
	SRV_FORMAT = "[CTRL-SRV %d] "
)

var (
	LogFile    = 0
	LogConsole = 1
	LogClient  = 0
	LogServer  = 0
)

func init() {
	if LogFile == 1 {
		logger.EnableLogger()
	}
}

func (c *Client) info(format string, info ...interface{}) {
	c.log(LOG_INFO, format, info...)
}

func (c *Client) error(format string, info ...interface{}) {
	c.log(LOG_ERROR, format, info...)
}

func (c *Client) log(prefix, format string, info ...interface{}) {
	if LogClient == 1 {
		msg := fmt.Sprintf("%s %v ", prefix, time.Now().Format("15:04:05.000"))
		msg += fmt.Sprintf(CLI_FORMAT, c.Uid)
		msg += fmt.Sprintf(format, info...)
		msg += "\n"
		if LogFile == 1 {
			logger.Write(msg)
		}
		if LogConsole == 1 {
			print(msg)
		}
	}
}

func (sc *ShardCtrler) info(format string, info ...interface{}) {
	sc.log(LOG_INFO, format, info...)
}

func (sc *ShardCtrler) error(format string, info ...interface{}) {
	sc.log(LOG_ERROR, format, info...)
}

func (sc *ShardCtrler) log(prefix, format string, info ...interface{}) {
	if LogServer == 1 {
		msg := fmt.Sprintf("%s %v C:%d ", prefix, time.Now().Format("15:04:05.000"), sc.configs[len(sc.configs)-1].Idx)
		msg += fmt.Sprintf(SRV_FORMAT, sc.me)
		msg += fmt.Sprintf(format, info...)
		msg += "\n"
		if LogFile == 1 {
			logger.Write(msg)
		}
		if LogConsole == 1 {
			print(msg)
		}
	}
}

func Debug(format string, info ...interface{}) {
	msg := fmt.Sprintf("%v", time.Now().Format("15:04:05.000"))
	msg += fmt.Sprintf(format, info...)
	msg += "\n"
	if LogFile == 1 {
		logger.Write(msg)
	}
	if LogConsole == 1 {
		print(msg)
	}
}
