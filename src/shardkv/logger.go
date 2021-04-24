package shardkv

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
	CLI_FORMAT = "[KV-CLI %d] "
	SRV_FORMAT = "[KV-SRV %d-%d] "
)

var (
	LogFile    = 0
	LogConsole = 1
	LogClient  = 1
	LogServer  = 0
	InDebug    = 1
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
	if InDebug == 1 {
		panic(LOG_ERROR)
	}
}

func (c *Client) log(prefix, format string, info ...interface{}) {
	if LogClient == 1 && (LogFile == 1 || LogConsole == 1) {
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

func (kv *ShardKV) info(format string, info ...interface{}) {
	kv.log(LOG_INFO, format, info...)
}

func (kv *ShardKV) error(format string, info ...interface{}) {
	kv.log(LOG_ERROR, format, info...)
	if InDebug == 1 {
		panic(LOG_ERROR)
	}
}

func (kv *ShardKV) log(prefix, format string, info ...interface{}) {
	if LogServer == 1 && (LogFile == 1 || LogConsole == 1) {
		msg := fmt.Sprintf("%s %v %s ", prefix, time.Now().Format("15:04:05.000"), kv.conf.String())
		msg += fmt.Sprintf(SRV_FORMAT, kv.gid, kv.me)
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
	if LogFile == 1 || LogConsole == 1 {
		msg := fmt.Sprintf("%s %v ", LOG_DEBUG, time.Now().Format("15:04:05.000"))
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
