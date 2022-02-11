package operations

import "sync"

type Operation interface {
	Init() error
	Run()
}

func Run(op Operation, wg *sync.WaitGroup) error {
	err := op.Init()
	if err != nil {
		return err
	}

	wg.Add(1)

	// Run the rest of the operation in the background.
	go func() {
		op.Run()
		wg.Done()
	}()

	return nil
}
