package main

import (
	"fmt"
	"math"
	"math/cmplx"
)

// Discrete Fourier Transform (DFT)
func DFT(x []complex128) []complex128 {
	N := len(x)
	X := make([]complex128, N)
	for k := 0; k < N; k++ {
		for n := 0; n < N; n++ {
			omega := -2i * math.Pi * complex(float64(k), 0) * complex(float64(n), 0) / complex(float64(N), 0)
			X[k] += x[n] * cmplx.Exp(omega)
		}
	}
	return X
}

func main() {
	// Test input signal
	x := []complex128{1, 0, -1, 0}

	// Compute the DFT
	X := DFT(x)

	// Output the result
	for k, Xk := range X {
		fmt.Printf("X[%d] = %v\n", k, Xk)
	}
}

