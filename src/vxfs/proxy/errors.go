package proxy

import (
	"errors"
	"strings"
)

var (
	ErrHttpNameFormat = errors.New("http bad name format")
	ErrHttpPathFormat = errors.New("http bad path format")
	ErrHttpUploadBody = errors.New("http bad body in upload")

	ErrInvalidatePrameter  = errors.New("invalidate parameter")
	ErrNameServiceNoLive   = errors.New("name service no living")
	ErrNameServiceNoSpace  = errors.New("name service no space")
	ErrStoreServiceNoLive  = errors.New("store service no living")
	ErrStoreServiceNoSpace = errors.New("store service no space")
)

func isHttpBadRequest(err error) bool {
	return strings.HasPrefix(err.Error(), "http bad")
}
