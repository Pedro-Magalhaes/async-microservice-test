package stage

import (
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestJob_GetFuncArg(t *testing.T) {
	type fields struct {
		Begin   *Stage
		End     *Stage
		Work    func(DoneCancelArgGet)
		jobChan chan interface{}
		funcArg interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   interface{}
	}{
		{"nil value", fields{funcArg: nil}, nil},
		{"string value", fields{funcArg: "test"}, "test"},
		{"obj value", fields{funcArg: struct{ a, b int }{a: 1, b: 2}}, struct{ a, b int }{a: 1, b: 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := Job{
				Begin:   tt.fields.Begin,
				End:     tt.fields.End,
				Work:    tt.fields.Work,
				jobChan: tt.fields.jobChan,
				funcArg: tt.fields.funcArg,
			}
			if got := j.GetFuncArg(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Job.GetFuncArg() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJob_Done(t *testing.T) {
	st := CreateStage("testStage")
	st2 := CreateStage("testStage2")
	type fields struct {
		Begin         *Stage
		End           *Stage
		JobsRunning   int
		Dones         int
		shouldTimeout bool
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{"one job runnig, should end stage when done is called", fields{End: st, Begin: st, shouldTimeout: false, JobsRunning: 1, Dones: 1}},
		{"2 jobs runnig, should not end stage when done is called", fields{End: st, Begin: st, shouldTimeout: true, JobsRunning: 2, Dones: 1}},
		{"should not change waitgroup if end != begin", fields{End: st2, Begin: st, shouldTimeout: true, JobsRunning: 1, Dones: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := Job{
				Begin: tt.fields.Begin,
				End:   tt.fields.End,
			}
			j.End.WaitGroup.Add(tt.fields.JobsRunning)
			for i := 0; i < tt.fields.Dones; i++ {
				j.Done()
			}
			c := make(chan struct{})
			go func() {
				defer close(c)
				j.End.Wait()
			}()
			select {
			case <-c:
				if tt.fields.shouldTimeout == true {
					t.Fatal("should not finish normal")
				}
			case <-time.After(10 * time.Millisecond): // timeout
				if tt.fields.shouldTimeout == false {
					t.Fatal("should not timeout")
				}
			}
		})
	}
}

func TestJob_Canceled(t *testing.T) {
	type fields struct {
		Begin   *Stage
		End     *Stage
		Work    func(DoneCancelArgGet)
		jobChan chan interface{}
		funcArg interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   chan interface{}
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := Job{
				Begin:   tt.fields.Begin,
				End:     tt.fields.End,
				Work:    tt.fields.Work,
				jobChan: tt.fields.jobChan,
				funcArg: tt.fields.funcArg,
			}
			if got := j.Canceled(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Job.Canceled() = %v, want %v", got, tt.want)
			}
		})
	}
}

// func TestCreateStages(t *testing.T) {
// 	want := &Stages{}
// 	if got := CreateStages(); !reflect.DeepEqual(got, want) {
// 		t.Errorf("CreateStages() = %v, want %v", got, want)
// 	}

// }

func TestCreateStage(t *testing.T) {
	type args struct {
		id string
	}
	tests := []struct {
		name string
		args args
		want *Stage
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CreateStage(tt.args.id); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CreateStage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStage_Wait(t *testing.T) {
	type fields struct {
		Id           string
		WaitGroup    *sync.WaitGroup
		Jobs         []Job
		JobsToFinish []Job
		channel      chan bool
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stage{
				Id:           tt.fields.Id,
				WaitGroup:    tt.fields.WaitGroup,
				Jobs:         tt.fields.Jobs,
				JobsToFinish: tt.fields.JobsToFinish,
				channel:      tt.fields.channel,
			}
			s.Wait()
		})
	}
}

func TestStage_AddJob(t *testing.T) {
	type fields struct {
		Id           string
		WaitGroup    *sync.WaitGroup
		Jobs         []Job
		JobsToFinish []Job
		channel      chan bool
	}
	type args struct {
		work    func(DoneCancelArgGet)
		workArg interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &Stage{
				Id:           tt.fields.Id,
				WaitGroup:    tt.fields.WaitGroup,
				Jobs:         tt.fields.Jobs,
				JobsToFinish: tt.fields.JobsToFinish,
				channel:      tt.fields.channel,
			}
			st.AddJob(tt.args.work, tt.args.workArg)
		})
	}
}

func TestStage_AddJobMultiStage(t *testing.T) {
	type fields struct {
		Id           string
		WaitGroup    *sync.WaitGroup
		Jobs         []Job
		JobsToFinish []Job
		channel      chan bool
	}
	type args struct {
		work     func(DoneCancelArgGet)
		endStage *Stage
		workArg  interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Job
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &Stage{
				Id:           tt.fields.Id,
				WaitGroup:    tt.fields.WaitGroup,
				Jobs:         tt.fields.Jobs,
				JobsToFinish: tt.fields.JobsToFinish,
				channel:      tt.fields.channel,
			}
			if got := st.AddJobMultiStage(tt.args.work, tt.args.endStage, tt.args.workArg); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Stage.AddJobMultiStage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStage_runWork(t *testing.T) {
	type fields struct {
		Id           string
		WaitGroup    *sync.WaitGroup
		Jobs         []Job
		JobsToFinish []Job
		channel      chan bool
	}
	type args struct {
		j Job
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &Stage{
				Id:           tt.fields.Id,
				WaitGroup:    tt.fields.WaitGroup,
				Jobs:         tt.fields.Jobs,
				JobsToFinish: tt.fields.JobsToFinish,
				channel:      tt.fields.channel,
			}
			st.runWork(tt.args.j)
		})
	}
}

func TestStage_Run(t *testing.T) {
	type fields struct {
		Id           string
		WaitGroup    *sync.WaitGroup
		Jobs         []Job
		JobsToFinish []Job
		channel      chan bool
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &Stage{
				Id:           tt.fields.Id,
				WaitGroup:    tt.fields.WaitGroup,
				Jobs:         tt.fields.Jobs,
				JobsToFinish: tt.fields.JobsToFinish,
				channel:      tt.fields.channel,
			}
			st.Run()
		})
	}
}

func TestStages_AddStage(t *testing.T) {
	type fields struct {
		stages      map[string]*Stage
		stagesArray []*Stage
	}
	type args struct {
		st *Stage
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stages{
				stages:      tt.fields.stages,
				stagesArray: tt.fields.stagesArray,
			}
			s.AddStage(tt.args.st)
		})
	}
}

func TestStages_AddStages(t *testing.T) {
	type fields struct {
		stages      map[string]*Stage
		stagesArray []*Stage
	}
	type args struct {
		stages []*Stage
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stages{
				stages:      tt.fields.stages,
				stagesArray: tt.fields.stagesArray,
			}
			s.AddStages(tt.args.stages)
		})
	}
}

func TestStages_GetStage(t *testing.T) {
	type fields struct {
		stages      map[string]*Stage
		stagesArray []*Stage
	}
	type args struct {
		id string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Stage
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stages{
				stages:      tt.fields.stages,
				stagesArray: tt.fields.stagesArray,
			}
			if got := s.GetStage(tt.args.id); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Stages.GetStage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStages_Run(t *testing.T) {
	type fields struct {
		stages      map[string]*Stage
		stagesArray []*Stage
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Stages{
				stages:      tt.fields.stages,
				stagesArray: tt.fields.stagesArray,
			}
			s.Run()
		})
	}
}
