package trackeroo

import "fmt"

var Jobs = map[string]func(map[string]any) map[string]any{
	"test": test,
}

func test(args map[string]any) map[string]any {
	var a, b int
	if aFloat, ok := args["a"].(float64); ok {
		a = int(aFloat)
	} else {
		return map[string]any{
			"error": "Missing parameter a",
		}
	}
	if bFloat, ok := args["b"].(float64); ok {
		b = int(bFloat)
	} else {
		return map[string]any{
			"error": "Missing parameter b",
		}
	}
	return map[string]any{
		"sum":    a + b,
		"sub":    a - b,
		"string": fmt.Sprintf("a: %d - b: %d", a, b),
	}
}
