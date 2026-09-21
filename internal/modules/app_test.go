package modules

import (
	"go.uber.org/fx"
	"testing"
)

func TestCompositionGraph(t *testing.T) {
	if err := fx.ValidateApp(App, fx.NopLogger); err != nil {
		t.Fatal(err)
	}
}
