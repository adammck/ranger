package fake_storage

import (
	"github.com/adammck/ranger/pkg/ranje"
	"github.com/adammck/ranger/pkg/roster/info"
)

type storage struct {
	infos []*info.RangeInfo
}

func NewFakeStorage(rangeInfos map[ranje.Ident]*info.RangeInfo) *storage {
	infos := []*info.RangeInfo{}
	for _, ri := range rangeInfos {
		infos = append(infos, ri)
	}

	return &storage{infos}
}

func (s *storage) Read() []*info.RangeInfo {
	return s.infos
}

func (s *storage) Write() {
}
