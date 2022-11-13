package fake_storage

import (
	"github.com/adammck/ranger/pkg/api"
)

type storage struct {
	infos []*api.RangeInfo
}

func NewFakeStorage(rangeInfos map[api.Ident]*api.RangeInfo) *storage {
	infos := []*api.RangeInfo{}
	for _, ri := range rangeInfos {
		infos = append(infos, ri)
	}

	return &storage{infos}
}

func (s *storage) Read() []*api.RangeInfo {
	return s.infos
}

func (s *storage) Write() {
}
