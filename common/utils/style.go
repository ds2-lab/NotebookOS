package utils

import (
	"fmt"
	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
	"os"
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
	CyanStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00FFFF"))
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
