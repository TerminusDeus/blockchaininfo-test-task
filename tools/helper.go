package tools

import "log"

func Check(errText string, err error) bool {
	if err != nil {
		log.Fatal(errText+" : ", err)
	}
	return true
}