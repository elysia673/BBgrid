package model

type Response struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data,omitempty"`
}

func Success(date interface{}) Response {
	return Response{
		Code: 0,
		Msg:  "success",
		Data: date,
	}
}

func Error(code int, msg string) Response {
	return Response{
		Code: code,
		Msg:  msg,
	}
}
