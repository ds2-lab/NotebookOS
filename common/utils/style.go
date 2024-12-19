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
	LightOrangeStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#ff9a59"))
	OrangeStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#ff7c28"))
	YellowStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#cc9500"))
	LightGreenStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#06ff00"))
	GreenStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#06cc00"))
	DarkGreenStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#055c03"))
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
	IncrementPendingStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#34e1ed"))
	DecrementPendingStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#2dcca1"))

	NotificationStyles = []lipgloss.Style{RedStyle, OrangeStyle, GrayStyle, GreenStyle}
)
