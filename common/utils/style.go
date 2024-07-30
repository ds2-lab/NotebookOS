package utils

import (
	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
)

func init() {
	lipgloss.SetColorProfile(termenv.ANSI256)
}

var (
	RedStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#cc0000"))
	OrangeStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#f46814"))
	YellowStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#cc9500"))
	GreenStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#06cc00"))
	LightBlueStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#3cc5ff"))
	BlueStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#0c00cc"))
	LightPurpleStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#d864ff"))
	PurpleStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#7400e0"))
	GrayStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#adadad"))

	NotificationStyles = []lipgloss.Style{RedStyle, OrangeStyle, GrayStyle, GreenStyle}
)
