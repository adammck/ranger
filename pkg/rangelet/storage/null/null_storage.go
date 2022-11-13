package null

import "github.com/adammck/ranger/pkg/api"

// NullStorage doesn't persist ranges. Read returns no ranges, and Write does
// nothing. This is useful for clients which don't care about range persistance
// across restarts.
type NullStorage struct {
}

func (s *NullStorage) Read() []*api.RangeInfo {
	return []*api.RangeInfo{}
}

func (s *NullStorage) Write() {
}
