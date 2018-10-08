package tns

import (
	"os"
)

type InvertFile struct {
	path string
	fp   *os.File
}
