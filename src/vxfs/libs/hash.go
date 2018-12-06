package libs

import (
	"crypto/sha1"
	"encoding/hex"
)

func HashSHA1(data []byte) string {
	sha := sha1.Sum(data)
	return hex.EncodeToString(sha[:])
}
