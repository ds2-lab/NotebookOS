package utils

import "os"

func ContextKey(name string) interface{} {
	return &name
}

func GetEnv(name string, def string) string {
	val := os.Getenv(name)
	if len(val) > 0 {
		return val
	} else {
		return def
	}
}
