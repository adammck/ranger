package discovery

import "fmt"

type Remote struct {
	Ident string
	Host  string
	Port  int
}

func (r *Remote) Addr() string {
	return fmt.Sprintf("%s:%d", r.Host, r.Port)
}
