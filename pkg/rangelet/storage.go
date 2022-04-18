package rangelet

import "github.com/adammck/ranger/pkg/roster/info"

type Storage interface {
	Read() []*info.RangeInfo
}
