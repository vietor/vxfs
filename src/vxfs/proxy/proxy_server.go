package proxy

import (
	"encoding/json"
	"io/ioutil"
	"net"
	"net/http"
	"regexp"
	"strings"
	"vxfs/dao/name"
	"vxfs/dao/store"
	"vxfs/libs"
)

type FileMeta struct {
	Mime string `json:"mime"`
	ETag string `json:"etag"`
}

type ProxyServer struct {
	closed   bool
	server   *http.Server
	listener net.Listener

	safeCode       string
	noDigMime      bool
	keyMaker       *libs.SnowFlake
	serviceManager *ServiceManager
}

type HttpHandler func(http.ResponseWriter, *http.Request)

func NewProxyServer(address string, safeCode string, noDigMime bool, keyMaker *libs.SnowFlake, serviceManager *ServiceManager) (s *ProxyServer, err error) {
	s = &ProxyServer{}
	s.safeCode = safeCode
	s.noDigMime = noDigMime
	s.keyMaker = keyMaker
	s.serviceManager = serviceManager

	if s.listener, err = net.Listen("tcp", address); err != nil {
		s.Close()
		s = nil
		return
	}

	serveMux := http.NewServeMux()
	s.server = &http.Server{
		Handler: serveMux,
	}
	serveMux.HandleFunc("/", s.route)
	return
}

func (s *ProxyServer) Serve() error {
	return s.server.Serve(s.listener)
}

func (s *ProxyServer) Close() {
	if s.listener != nil {
		s.listener.Close()
		s.listener = nil
	}
	if s.serviceManager != nil {
		s.serviceManager = nil
	}
}

func (s *ProxyServer) route(res http.ResponseWriter, req *http.Request) {
	var (
		write   bool
		handler HttpHandler
	)

	switch req.Method {
	case "PUT":
		write = true
		handler = s.handleUpload
	case "DELETE":
		write = true
		handler = s.handleDelete
	case "HEAD", "GET":
		handler = s.handleDownload
	default:
		http.Error(res, "Access PUT,DELETE,HEAD,GET", http.StatusMethodNotAllowed)
		return
	}

	if write && len(s.safeCode) > 0 {
		safeCode := req.Header.Get("VXFS-SAFE-CODE")
		if len(safeCode) < 1 || s.safeCode != safeCode {
			http.Error(res, "Require Header `VXFS-SAFE-CODE`", http.StatusUnauthorized)
			return
		}
	}
	handler(res, req)
}

func (s *ProxyServer) parseName(req *http.Request) (name string, err error) {
	path := req.URL.Path[1:]
	if m, _ := regexp.MatchString("^[0-9a-zA-Z_\\-.]{1,256}$", path); !m {
		err = ErrHttpNameFormat
		return
	}
	name = path
	return
}

func (s *ProxyServer) handleUpload(res http.ResponseWriter, req *http.Request) {
	var (
		err   error
		xdata = map[string]interface{}{}

		meta = &FileMeta{}

		nwreq = &name.WriteRequest{}
		nwres = &name.WriteResponse{}

		swreq = &store.WriteRequest{}
		swres = &store.WriteResponse{}

		ndreq = &name.DeleteRequest{}
		ndres = &name.DeleteResponse{}
	)
	defer httpSendJsonData(res, &err, xdata)

	if nwreq.Name, err = s.parseName(req); err != nil {
		return
	}

	if swreq.Data, err = ioutil.ReadAll(req.Body); err != nil || len(swreq.Data) < 1 {
		err = ErrHttpUploadBody
		return
	}
	req.Body.Close()
	meta.ETag = libs.HashSHA1(swreq.Data)

	if s.noDigMime {
		if meta.Mime = req.Header.Get("Content-Type"); meta.Mime == "" {
			meta.Mime = http.DetectContentType(swreq.Data)
		}
	} else {
		headerMime := req.Header.Get("Content-Type")
		meta.Mime = http.DetectContentType(swreq.Data)
		if len(headerMime) > 0 {
			if strings.HasPrefix(meta.Mime, "text/") || meta.Mime == "application/octet-stream" {
				meta.Mime = headerMime
			}
		}
	}

	if swreq.Meta, err = json.Marshal(meta); err != nil {
		return
	}

	if nwreq.Key, err = s.keyMaker.NextId(); err != nil {
		return
	}

	if nwreq.Sid, err = s.serviceManager.GetSid(int64(len(swreq.Data))); err != nil {
		return
	}

	if err = s.serviceManager.WriteName(nwreq, nwres); err != nil {
		return
	}

	swreq.Key = nwreq.Key
	if err = s.serviceManager.WriteStore(nwreq.Sid, swreq, swres); err != nil {
		ndreq.Name = nwreq.Name
		s.serviceManager.DeleteName(ndreq, ndres)
		return
	}
}

func (s *ProxyServer) handleDelete(res http.ResponseWriter, req *http.Request) {
	var (
		err   error
		xdata = map[string]interface{}{}

		nreq = &name.ReadRequest{}
		nres = &name.ReadResponse{}

		ndreq = &name.DeleteRequest{}
		ndres = &name.DeleteResponse{}

		sdreq = &store.DeleteRequest{}
		sdres = &store.DeleteResponse{}
	)
	defer httpSendJsonData(res, &err, xdata)

	if nreq.Name, err = s.parseName(req); err != nil {
		return
	}

	if err = s.serviceManager.ReadName(nreq, nres); err != nil {
		if libs.IsErrorSame(err, name.ErrNameNotExists) {
			err = nil
		}
		return
	}

	sdreq.Key = nres.Key
	if err = s.serviceManager.DeleteStore(nres.Sid, sdreq, sdres); err != nil {
		if !libs.IsErrorSame(err, store.ErrStoreNotExists) {
			return
		}
	}

	ndreq.Name = nreq.Name
	if err = s.serviceManager.DeleteName(ndreq, ndres); err != nil {
		return
	}
}

func (s *ProxyServer) handleDownload(res http.ResponseWriter, req *http.Request) {
	var (
		err   error
		mime  string
		xdata []byte

		meta FileMeta

		nreq = &name.ReadRequest{}
		nres = &name.ReadResponse{}

		sreq = &store.ReadRequest{}
		sres = &store.ReadResponse{}
	)
	defer httpSendByteData(res, &err, &mime, &xdata)

	if nreq.Name, err = s.parseName(req); err != nil {
		return
	}

	if err = s.serviceManager.ReadName(nreq, nres); err != nil {
		return
	}

	sreq.Key = nres.Key
	if err = s.serviceManager.ReadStore(nres.Sid, sreq, sres); err != nil {
		return
	}

	if err = json.Unmarshal(sres.Meta, &meta); err != nil {
		return
	}

	mime = meta.Mime
	xdata = sres.Data
	res.Header().Set("ETag", meta.ETag)
}
