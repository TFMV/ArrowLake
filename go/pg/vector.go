package pg

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
)

type Vector struct {
	vec []float32
}

func NewVector(vec []float32) Vector {
	return Vector{vec: vec}
}

func (v Vector) Slice() []float32 {
	return v.vec
}

func (v Vector) String() string {
	var buf strings.Builder
	buf.WriteByte('[')

	for i, val := range v.vec {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(strconv.FormatFloat(float64(val), 'f', -1, 32))
	}

	buf.WriteByte(']')
	return buf.String()
}

func (v *Vector) Parse(s string) error {
	s = strings.Trim(s, "[]")
	if s == "" {
		v.vec = []float32{}
		return nil
	}

	sp := strings.Split(s, ",")
	v.vec = make([]float32, len(sp))
	for i, strVal := range sp {
		n, err := strconv.ParseFloat(strVal, 32)
		if err != nil {
			return fmt.Errorf("parse vector: %w", err)
		}
		v.vec[i] = float32(n)
	}
	return nil
}

var _ sql.Scanner = (*Vector)(nil)

func (v *Vector) Scan(src interface{}) error {
	switch src := src.(type) {
	case []byte:
		return v.Parse(string(src))
	case string:
		return v.Parse(src)
	default:
		return fmt.Errorf("unsupported data type: %T", src)
	}
}

var _ driver.Valuer = (*Vector)(nil)

func (v Vector) Value() (driver.Value, error) {
	return v.String(), nil
}

// Example usage
func main() {
	// Example usage of Vector
	vec := NewVector([]float32{0.1, 0.2, 0.3})
	fmt.Println(vec.String()) // Output: [0.1,0.2,0.3]
}
