package wsclient

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

// DecodeComResponse decodes the common wrapper:
// ComResponse { int32 code = 1; bytes data = 2; }.
func DecodeComResponse(data []byte) (int32, []byte, error) {
	var code int32
	var body []byte
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return 0, nil, fmt.Errorf("解析 ComResponse tag 失败")
		}
		data = data[n:]
		switch num {
		case 1:
			if typ != protowire.VarintType {
				return 0, nil, fmt.Errorf("ComResponse.code wire type 错误")
			}
			v, n := protowire.ConsumeVarint(data)
			if n < 0 {
				return 0, nil, fmt.Errorf("解析 ComResponse.code 失败")
			}
			code = int32(v)
			data = data[n:]
		case 2:
			if typ != protowire.BytesType {
				return 0, nil, fmt.Errorf("ComResponse.data wire type 错误")
			}
			v, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return 0, nil, fmt.Errorf("解析 ComResponse.data 失败")
			}
			body = append(body[:0], v...)
			data = data[n:]
		default:
			n := protowire.ConsumeFieldValue(num, typ, data)
			if n < 0 {
				return 0, nil, fmt.Errorf("解析 ComResponse 未知字段失败")
			}
			data = data[n:]
		}
	}
	return code, body, nil
}
