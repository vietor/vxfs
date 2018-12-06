package proxy

import (
	"encoding/json"
	"net/http"
	"strconv"
	"vxfs/dao/name"
	"vxfs/dao/store"
	"vxfs/libs"
)

func errorToHttpStatus(err error) int {
	if isHttpBadRequest(err) {
		return 400
	} else if libs.IsErrorSame(err, name.ErrNameNotExists) {
		return 404
	} else if libs.IsErrorSame(err, store.ErrStoreNotExists) {
		return 404
	}
	return 500
}

func errorToErrorCode(err error) int {
	if isHttpBadRequest(err) {
		return 101
	} else if libs.IsErrorSame(err, name.ErrNameNotExists) {
		return 102
	} else if libs.IsErrorSame(err, store.ErrStoreNotExists) {
		return 103
	} else if libs.IsErrorSame(err, name.ErrNameKeyExists) {
		return 104
	} else if libs.IsErrorSame(err, store.ErrStoreKeyExists) {
		return 105
	}
	return 100
}

func httpSendByteData(res http.ResponseWriter, err *error, mime *string, data *[]byte) {
	if *err != nil {
		result := map[string]interface{}{}
		result["code"] = errorToErrorCode(*err)
		result["error"] = (*err).Error()
		errorBody, _ := json.Marshal(result)
		res.Header().Set("Content-Type", "application/json")
		res.WriteHeader(errorToHttpStatus(*err))
		res.Write(errorBody)
	} else {
		header := res.Header()
		if len(*mime) > 0 {
			header.Set("Content-Type", *mime)
		} else {
			header.Set("Content-Type", "application/octet-stream")
		}
		header.Set("Content-Length", strconv.Itoa(len(*data)))
		res.Write(*data)
	}
}

func httpSendJsonData(res http.ResponseWriter, err *error, data map[string]interface{}) {
	res.Header().Set("Content-Type", "application/json;charset=utf-8")
	result := map[string]interface{}{}
	if *err == nil {
		result["code"] = 0
		result["data"] = data
	} else {
		result["code"] = errorToErrorCode(*err)
		result["error"] = (*err).Error()
	}
	json.NewEncoder(res).Encode(result)
}
