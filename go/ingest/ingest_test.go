package main

import (
	"testing"
)

func TestParseVector(t *testing.T) {
	tests := []struct {
		input    string
		expected Vector
		err      bool
	}{
		{"item1,0.5,1.2,3.4,2.0,1.1,0.9", Vector{Name: "item1", Embedding: []float32{0.5, 1.2, 3.4, 2.0, 1.1, 0.9}}, false},
		{"item2,2.3,3.5,5.6,4.4", Vector{}, true}, // Incorrect number of dimensions
		{"invalid,data,here", Vector{}, true},     // Invalid data
	}
	for _, test := range tests {
		result, err := ParseVector(test.input)
		if (err != nil) != test.err {
			t.Errorf("ParseVector(%q) unexpected error status: %v", test.input, err)
		}
		if !test.err && !vectorsEqual(result, test.expected) {
			t.Errorf("ParseVector(%q) = %v, want %v", test.input, result, test.expected)
		}
	}
}

func vectorsEqual(a, b Vector) bool {
	if a.Name != b.Name || len(a.Embedding) != len(b.Embedding) {
		return false
	}
	for i := range a.Embedding {
		if a.Embedding[i] != b.Embedding[i] {
			return false
		}
	}
	return true
}
