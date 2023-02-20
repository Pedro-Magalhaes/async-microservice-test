package model

type TestInterface interface {
	// Função que vai rodar antes da mainFunc
	setup()
	// Função que roda depois da mainFunc
	terdown()
	// Função que faz declara os testes
	mainFunc()
}
