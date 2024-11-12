package main

import (
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/charmbracelet/lipgloss"
)

var (
	RedStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#EE4266"))
	OrangeStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#FFA113"))
	YellowStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#FFD23F"))
	GreenStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#2A9D8F"))
	LightBlueStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#3185FC"))
	BlueStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#0A64E2"))
	PurpleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#8400D6"))
)

type foo struct {
	log logger.Logger
}

func newFoo() *foo {
	f := &foo{}

	config.InitLogger(&f.log, f)

	return f
}

func (f *foo) test(name string) {
	f.log.Info(RedStyle.Render("hello, %s"), name)
	f.log.Info(OrangeStyle.Render("hello, %s"), name)
	f.log.Info(YellowStyle.Render("hello, %s"), name)
	f.log.Info(GreenStyle.Render("hello, %s"), name)
	f.log.Info(LightBlueStyle.Render("hello, %s"), name)
	f.log.Info(BlueStyle.Render("hello, %s"), name)
	f.log.Info(PurpleStyle.Render("hello, %s"), name)
}

func main() {
	config.LogLevel = logger.LOG_LEVEL_ALL

	f := newFoo()

	f.test("ben")
}
