package main

import (
	"fmt"
	"testing"
)

func TestVectorToString(t *testing.T) {
	tests := []struct {
		vector   Vector
		expected string
	}{
		{Vector{Name: "item1", Embedding: []float32{0.5, 1.2, 3.4, 2.0, 1.1, 0.9}}, "[0.5, 1.2, 3.4, 2.0, 1.1, 0.9]"},
		{Vector{Name: "item2", Embedding: []float32{2.3, 3.5, 5.6, 4.4}}, "[2.3, 3.5, 5.6, 4.4]"},
		{Vector{Name: "item3", Embedding: []float32{1.0, 1.0, 1.0, 1.0, 1.0, 1.0}}, "[1.0, 1.0, 1.0, 1.0, 1.0, 1.0]"},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("VectorToString for %s", tc.vector.Name), func(t *testing.T) {
			if result := vectorToString(tc.vector); result != tc.expected {
				t.Errorf("vectorToString() = %s, expected %s", result, tc.expected)
			}
		})
	}
}

func TestConvertVectorsToPgvectorFormat(t *testing.T) {
	tests := []struct {
		vectors  []Vector
		expected [][]interface{}
	}{
		{
			[]Vector{
				{Name: "item1", Embedding: []float32{0.5, 1.2, 3.4, 2.0, 1.1, 0.9}},
				{Name: "item2", Embedding: []float32{2.3, 3.5, 5.6, 4.4}},
				{Name: "item3", Embedding: []float32{1.0, 1.0, 1.0, 1.0, 1.0, 1.0}},
			},
			[][]interface{}{
				{"item1", "[0.5, 1.2, 3.4, 2.0, 1.1, 0.9]"},
				{"item2", "[2.3, 3.5, 5.6, 4.4]"},
				{"item3", "[1.0, 1.0, 1.0, 1.0, 1.0, 1.0]"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("ConvertVectorsToPgvectorFormat for %d vectors", len(tc.vectors)), func(t *testing.T) {
			if result := convertVectorsToPgvectorFormat(tc.vectors); !equal(result, tc.expected) {
				t.Errorf("convertVectorsToPgvectorFormat() = %v, expected %v", result, tc.expected)
			}
		})
	}
}
