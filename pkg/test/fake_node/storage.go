package fake_node

import "github.com/adammck/ranger/pkg/roster/info"

type Storage struct {
	infos []*info.RangeInfo
}

func (s *Storage) Read() []*info.RangeInfo {
	return s.infos
}

func (s *Storage) Write() {
}
