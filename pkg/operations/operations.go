package operations

type Operation interface {
	Init() error
	Run()
}

func Run(op Operation) error {
	err := op.Init()
	if err != nil {
		return err
	}

	// Run the rest of the operation in the background.
	go op.Run()
	return nil
}
