package main

import (
	"fmt"
	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
	"os"
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
	GrayStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#adadad"))
	PurpleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#8400D6"))
	LightPurpleStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("#d864ff"))
)

func init() {
	lipgloss.SetColorProfile(termenv.ANSI256)

	// https://no-color.org/
	//
	// If there is a NO_COLOR environment variable, then colored output is disabled.
	noColorVal := os.Getenv("NO_COLOR")
	if noColorVal != "" {
		fmt.Printf("[INFO] Found non-empty value for \"NO_COLOR\" environment variable: \"%s\".\n"+
			"Disabling colored output.\n", noColorVal)

		RedStyle = RedStyle.Foreground(lipgloss.NoColor{})
		OrangeStyle = OrangeStyle.Foreground(lipgloss.NoColor{})
		YellowStyle = YellowStyle.Foreground(lipgloss.NoColor{})
		GreenStyle = GreenStyle.Foreground(lipgloss.NoColor{})
		LightBlueStyle = LightBlueStyle.Foreground(lipgloss.NoColor{})
		BlueStyle = BlueStyle.Foreground(lipgloss.NoColor{})
		LightPurpleStyle = LightPurpleStyle.Foreground(lipgloss.NoColor{})
		PurpleStyle = PurpleStyle.Foreground(lipgloss.NoColor{})
		GrayStyle = GrayStyle.Foreground(lipgloss.NoColor{})
	}
}

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
