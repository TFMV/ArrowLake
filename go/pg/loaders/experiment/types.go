package main

import (
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgtype"
)

func ByteaArray(bytesArray [][]byte) *pgtype.ByteaArray {
	pgtypeByteaArray := make([]pgtype.Bytea, len(bytesArray))
	for i, byteSlice := range bytesArray {
		pgtypeByteaArray[i].Bytes = byteSlice
		pgtypeByteaArray[i].Status = pgtype.Present
	}
	return &pgtype.ByteaArray{
		Elements:   pgtypeByteaArray,
		Dimensions: []pgtype.ArrayDimension{{Length: int32(len(bytesArray)), LowerBound: 1}},
		Status:     pgtype.Present,
	}
}

func NullByteaArray(bytesArray [][]byte) *pgtype.ByteaArray {
	pgtypeByteaArray := make([]pgtype.Bytea, len(bytesArray))
	for i, byteSlice := range bytesArray {
		pgtypeByteaArray[i].Bytes = byteSlice
		if byteSlice == nil {
			pgtypeByteaArray[i].Status = pgtype.Null
		} else {
			pgtypeByteaArray[i].Status = pgtype.Present
		}
	}
	return &pgtype.ByteaArray{
		Elements:   pgtypeByteaArray,
		Dimensions: []pgtype.ArrayDimension{{Length: int32(len(bytesArray)), LowerBound: 1}},
		Status:     pgtype.Present,
	}
}

func TextArray(stringSlice []string) *pgtype.TextArray {
	pgtypeTextArray := make([]pgtype.Text, len(stringSlice))
	for i, s := range stringSlice {
		pgtypeTextArray[i].String = s
		pgtypeTextArray[i].Status = pgtype.Present
	}
	return &pgtype.TextArray{
		Elements:   pgtypeTextArray,
		Dimensions: []pgtype.ArrayDimension{{Length: int32(len(stringSlice)), LowerBound: 1}},
		Status:     pgtype.Present,
	}
}

func TimestampTZArray(timeSlice []time.Time) *pgtype.TimestamptzArray {
	pgtypeTimestamptzArray := make([]pgtype.Timestamptz, len(timeSlice))
	for i, t := range timeSlice {
		pgtypeTimestamptzArray[i].Time = t
		pgtypeTimestamptzArray[i].Status = pgtype.Present
	}
	return &pgtype.TimestamptzArray{
		Elements:   pgtypeTimestamptzArray,
		Dimensions: []pgtype.ArrayDimension{{Length: int32(len(timeSlice)), LowerBound: 1}},
		Status:     pgtype.Present,
	}
}

func NullTimestampTZArray(timeSlice []*time.Time) *pgtype.TimestamptzArray {
	pgtypeTimestamptzArray := make([]pgtype.Timestamptz, len(timeSlice))
	for i, t := range timeSlice {
		if t == nil {
			pgtypeTimestamptzArray[i].Status = pgtype.Null
		} else {
			pgtypeTimestamptzArray[i].Time = *t
			pgtypeTimestamptzArray[i].Status = pgtype.Present
		}
	}
	return &pgtype.TimestamptzArray{
		Elements:   pgtypeTimestamptzArray,
		Dimensions: []pgtype.ArrayDimension{{Length: int32(len(timeSlice)), LowerBound: 1}},
		Status:     pgtype.Present,
	}
}

func DateArray(timeSlice []time.Time) *pgtype.DateArray {
	pgtypeDateArray := make([]pgtype.Date, len(timeSlice))
	for i, t := range timeSlice {
		pgtypeDateArray[i].Time = t
		pgtypeDateArray[i].Status = pgtype.Present
	}
	return &pgtype.DateArray{
		Elements:   pgtypeDateArray,
		Dimensions: []pgtype.ArrayDimension{{Length: int32(len(timeSlice)), LowerBound: 1}},
		Status:     pgtype.Present,
	}
}

func Int2Array(ints []int16) *pgtype.Int2Array {
	pgtypeInt2Array := make([]pgtype.Int2, len(ints))
	for i, someInt := range ints {
		pgtypeInt2Array[i].Int = someInt
		pgtypeInt2Array[i].Status = pgtype.Present
	}
	return &pgtype.Int2Array{
		Elements:   pgtypeInt2Array,
		Dimensions: []pgtype.ArrayDimension{{Length: int32(len(ints)), LowerBound: 1}},
		Status:     pgtype.Present,
	}
}

func Int4Array(ints []int32) *pgtype.Int4Array {
	pgtypeInt4Array := make([]pgtype.Int4, len(ints))
	for i, someInt := range ints {
		pgtypeInt4Array[i].Int = someInt
		pgtypeInt4Array[i].Status = pgtype.Present
	}
	return &pgtype.Int4Array{
		Elements:   pgtypeInt4Array,
		Dimensions: []pgtype.ArrayDimension{{Length: int32(len(ints)), LowerBound: 1}},
		Status:     pgtype.Present,
	}
}

func Int8Array(bigInts []int64) *pgtype.Int8Array {
	pgtypeInt8Array := make([]pgtype.Int8, len(bigInts))
	for i, bigInt := range bigInts {
		pgtypeInt8Array[i].Int = bigInt
		pgtypeInt8Array[i].Status = pgtype.Present
	}
	return &pgtype.Int8Array{
		Elements:   pgtypeInt8Array,
		Dimensions: []pgtype.ArrayDimension{{Length: int32(len(bigInts)), LowerBound: 1}},
		Status:     pgtype.Present,
	}
}

func Float8Array(floats []float64) *pgtype.Float8Array {
	pgtypeFloat8Array := make([]pgtype.Float8, len(floats))
	for i, someFloat := range floats {
		pgtypeFloat8Array[i].Float = someFloat
		pgtypeFloat8Array[i].Status = pgtype.Present
	}
	return &pgtype.Float8Array{
		Elements:   pgtypeFloat8Array,
		Dimensions: []pgtype.ArrayDimension{{Length: int32(len(floats)), LowerBound: 1}},
		Status:     pgtype.Present,
	}
}
func UUIDArray(uuids []uuid.UUID) *pgtype.ByteaArray {
	if uuids == nil {
		return &pgtype.ByteaArray{Status: pgtype.Null}
	}
	pgtypeByteaArray := make([]pgtype.Bytea, len(uuids))
	for i, uuid := range uuids {
		uuidCopy := uuid
		pgtypeByteaArray[i].Bytes = uuidCopy[:]
		pgtypeByteaArray[i].Status = pgtype.Present
	}
	return &pgtype.ByteaArray{
		Elements:   pgtypeByteaArray,
		Dimensions: []pgtype.ArrayDimension{{Length: int32(len(uuids)), LowerBound: 1}},
		Status:     pgtype.Present,
	}
}
