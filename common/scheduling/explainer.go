package scheduling

const (
	ExplainInteractivePriority ExplainerEntry = "explain_ip"
	ExplainPreemptionPriority  ExplainerEntry = "explain_pp"
	ExplainScaleOutPriority    ExplainerEntry = "explain_sop"
)

type ExplainerEntry string

type Explainer interface {
	Explain(key ExplainerEntry) string
}
