package stage

import (
	"log"
	"sync"
	"time"
)

type DoneCancelArgGet interface {
	Done()
	GetFuncArg() interface{}
	Canceled() chan interface{}
}

type Stage struct {
	Id           string
	WaitGroup    *sync.WaitGroup
	Jobs         []Job // Tenho que criar um array pros que pertencem ao stage e um para os que precisam ser finalizados
	JobsToFinish []Job
	channel      chan bool
}

type Stages struct {
	stages      map[string]*Stage
	stagesArray []*Stage
}

type Job struct {
	Begin   *Stage
	End     *Stage
	Work    func(DoneCancelArgGet)
	jobChan chan interface{}
	funcArg interface{}
}

func (j Job) GetFuncArg() interface{} {
	return j.funcArg
}

func (j Job) Done() {
	j.jobChan <- true
	if j.Begin == j.End {
		j.End.WaitGroup.Done()
	}
}

func (j Job) Wait() chan interface{} {
	return j.jobChan
}

func (j Job) Canceled() chan interface{} {
	return j.jobChan
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
		channel:   make(chan bool),
		Jobs:      []Job{},
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
		jobChan: make(chan interface{}),
		Work:    work,
		End:     endStage,
		Begin:   st,
		funcArg: workArg,
	}
	if st == endStage {
		log.Default().Println("Adicionando ao wait Group", st.Id)
		endStage.WaitGroup.Add(1)
	}

	st.Jobs = append(st.Jobs, job)
	println("Len de jobs", len(st.Jobs))
	return &job
}

func (st *Stage) runWork(j Job) {
	go j.Work(&j)
	//println("Job finalizado. Check se job.WG == stage.WG", j.WaitGroup == st.WaitGroup)
	select {
	case <-j.Wait():
		log.Default().Println("End channel")
	case <-time.After(15 * time.Second):
		// colocar em uma propriedade do Stage ou do Job?
		// como remover pros jobs multistage?
		log.Default().Println("Job timeout 15s")
	}
	// if j.Begin == j.End {
	// 	j.End.WaitGroup.Done() // como colocar um WG
	// }
}

// Roda um estágio
func (st *Stage) Run() {
	for _, job := range st.Jobs {
		go st.runWork(job)
	}
	st.Wait()
	log.Default().Println("Finalizando stage: ", st.Id)
	// FIXME: melhorar logica, usar apenas um canal ou preciso de uma para cada job?
	for _, job := range st.Jobs {
		close(job.jobChan)
	}
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

// colocar um padrão usando select para jobs que não terminam e precisam ser interrompidos ao final de um "stage"
// Nesse caso não adicionamos ele em nenhum WG
