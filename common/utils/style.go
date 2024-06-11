package utils

import (
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
