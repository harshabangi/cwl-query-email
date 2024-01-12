package pipeline

type Pipeline interface {
	AddStage(numWorkers int, workFn func(inObj interface{}) (interface{}, error))
	Run(input []interface{}) ([]interface{}, error)
}

type (
	pipeline struct {
		inputChan  chan dataOrError
		outputChan chan dataOrError
		stages     []stageInfo
	}

	dataOrError struct {
		Data  interface{}
		Error error
	}

	stageInfo struct {
		numWorkers int
		workFn     func(input interface{}) (interface{}, error)

		inChan  chan dataOrError
		outChan chan dataOrError
	}
)

func New() Pipeline {
	return &pipeline{
		inputChan:  make(chan dataOrError),
		outputChan: make(chan dataOrError),
	}
}

func (p *pipeline) AddStage(numWorkers int, workFn func(input interface{}) (interface{}, error)) {
	var (
		n      = len(p.stages)
		inChan chan dataOrError
		outCh  = make(chan dataOrError)
	)

	if n == 0 {
		inChan = p.inputChan
	} else {
		inChan = p.stages[n-1].outChan
	}

	s := stageInfo{
		numWorkers: numWorkers,
		workFn:     workFn,
		inChan:     inChan,
		outChan:    outCh,
	}

	go func() {
		defer close(outCh)

		for in := range inChan {
			if in.Error != nil {
				outCh <- dataOrError{
					Error: in.Error,
				}
				return
			}

			out, err := s.workFn(in.Data)
			outCh <- dataOrError{
				Data:  out,
				Error: err,
			}
			if err != nil {
				return
			}
		}
	}()

	p.outputChan = outCh
	p.stages = append(p.stages, s)
}

func (p *pipeline) Run(input []interface{}) ([]interface{}, error) {

	go func() {
		defer close(p.inputChan)
		for _, in := range input {
			p.inputChan <- dataOrError{Data: in}
		}
	}()

	var result []interface{}

	for out := range p.outputChan {
		if out.Error != nil {
			return nil, out.Error
		}
		result = append(result, out.Data)
	}

	return result, nil
}
