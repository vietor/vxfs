package libs

func IsErrorSame(err1 error, err2 error) bool {
	return err1.Error() == err2.Error()
}
