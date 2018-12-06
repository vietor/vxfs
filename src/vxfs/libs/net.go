package libs

import (
	"errors"
	"net"
)

func IsHostPort(hostport string) bool {
	_, _, err := net.SplitHostPort(hostport)
	return err == nil
}

func IsStrictHostPort(hostport string) bool {
	host, _, err := net.SplitHostPort(hostport)
	return err == nil && len(host) > 0
}

func GetExternalIP() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return "", err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue // not an ipv4 address
			}
			return ip.String(), nil
		}
	}
	return "", errors.New("are you connected to the network?")
}

func GetPublicHostPort(hostport string) (output string, err error) {
	var host, port string
	if host, port, err = net.SplitHostPort(hostport); err != nil {
		return
	}
	if host == "" || host == "0.0.0.0" {
		if host, err = GetExternalIP(); err != nil {
			err = nil
			host = "127.0.0.1"
			return
		}
	}
	output = host + ":" + port
	return
}
