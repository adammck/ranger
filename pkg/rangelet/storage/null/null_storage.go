package null

import "github.com/adammck/ranger/pkg/roster/info"

// NullStorage doesn't persist ranges. Read returns no ranges, and Write does
// nothing. This is useful for clients which don't care about range persistance
// across restarts.
type NullStorage struct {
}

func (s *NullStorage) Read() []*info.RangeInfo {
	return []*info.RangeInfo{}
}

func (s *NullStorage) Write() {
}
