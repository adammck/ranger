package api

type Storage interface {
	Read() []*RangeInfo
	Write()
}
