package stage

import (
	"log"
	"sync"
)

type DoneCancelArgGet interface {
	Done()
	GetFuncArg() interface{}
	Canceled() chan interface{}
}

type Stage struct {
	Id           string
	WaitGroup    *sync.WaitGroup
	Jobs         []*Job
	channel      chan interface{}
}

type Stages struct {
	stages      map[string]*Stage
	stagesArray []*Stage
}

type Job struct {
	Begin   *Stage
	End     *Stage
	Work    func(DoneCancelArgGet)
	funcArg interface{}
	hasFinished bool
}

func (j *Job) GetFuncArg() interface{} {
	return j.funcArg
}

func (j *Job) Done() {
	if j.Begin == j.End && !j.hasFinished {
		j.hasFinished = true //evita que o mesmo job chame done() mais de 1 vez
		j.End.WaitGroup.Done()
	}
}

func (j *Job) Canceled() chan interface{} {
	return j.End.channel
}

func CreateStages() *Stages {
	return &Stages{
		make(map[string]*Stage),
		[]*Stage{},
	}
}

func CreateStage(id string) *Stage {
	return &Stage{
		Id:        id,
		WaitGroup: &sync.WaitGroup{},
		channel:   make(chan interface{}),
		Jobs:      []*Job{},
	}
}

func (s *Stage) Wait() {
	s.WaitGroup.Wait()
}

// Adicona uma tarefa ao estágio
func (st *Stage) AddJob(work func(DoneCancelArgGet), workArg interface{}) {
	st.AddJobMultiStage(work, st, workArg)
}

// Adiciona uma tarefa que inicia em um estágio mas termina em outro
func (st *Stage) AddJobMultiStage(work func(DoneCancelArgGet), endStage *Stage, workArg interface{}) *Job {
	job := Job{
		Work:    work,
		End:     endStage,
		Begin:   st,
		funcArg: workArg,
		hasFinished: false,
	}
	if st == endStage {
		log.Default().Println("Adicionando ao wait Group", st.Id)
		endStage.WaitGroup.Add(1)
	}
	st.Jobs = append(st.Jobs, &job)
	return &job
}

func (st *Stage) runWork(j *Job) {
	go j.Work(j)
}

// Roda um estágio
func (st *Stage) Run() {
	for _, job := range st.Jobs {
		go st.runWork(job)
	}
	st.Wait()
	log.Default().Println("Finalizando stage: ", st.Id)
	close(st.channel)
}

// Adiciona um estágio
func (s *Stages) AddStage(st *Stage) {
	s.stages[st.Id] = st
}

// Adiciona um array de estágios
func (s *Stages) AddStages(stages []*Stage) {
	s.stagesArray = append(s.stagesArray, stages...)
	for _, st := range stages {
		s.stages[st.Id] = st
	}
}

// retorna um estágio dado um id
func (s *Stages) GetStage(id string) *Stage {
	return s.stages[id]
}

// Roda todos os estágios internos
func (s *Stages) Run() {
	for _, s := range s.stagesArray {
		log.Default().Println("Runnig stage: ", s.Id)
		s.Run()
	}
}
