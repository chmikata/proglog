package main

import (
	"encoding/csv"
	"fmt"
	"math"
	"strings"

	"github.com/chmikata/proglog/csvconvert"
	// "github.com/chmikata/proglog/csvdecorate"
)

type Hoger interface {
	Exec(m string) string
}

type HogeAdapter func(m string) string

func (h HogeAdapter) Exec(m string) string {
	return h(m)
}

func TestExec(h Hoger, m string) {
	fmt.Println("Test exec")
	fmt.Println(h.Exec(m))
}

func FugaExec(m string) string {
	fmt.Printf("FugaExec: %s\n", m)
	return m
}

type MyData struct {
	Name   string `csv:"name"`
	Age    int    `csv:"age"`
	HasPet bool   `csv:"has_pet"`
}

func Filter[T any](s []T, f func(T) bool) []T {
	var r []T
	for _, v := range s {
		if f(v) {
			r = append(r, v)
		}
	}
	return r
}

func Map[T1, T2 any](s []T1, f func(T1) T2) []T2 {
	r := make([]T2, len(s))
	for i, v := range s {
		r[i] = f(v)
	}
	return r
}

type Pair[T fmt.Stringer] struct {
	Val1 T
	Val2 T
}

type Differ[T any] interface {
	fmt.Stringer
	Diff(T) float64
}

func FindCloser[T Differ[T]](pair1, pair2 Pair[T]) Pair[T] {
	d1 := pair1.Val1.Diff(pair1.Val2)
	d2 := pair2.Val1.Diff(pair2.Val2)
	if d1 > d2 {
		return pair1
	}
	return pair2
}

type Pair2D struct {
	X, Y int
}

func (p Pair2D) String() string {
	return fmt.Sprintf("{%d, %d}", p.X, p.Y)
}

func (p Pair2D) Diff(from Pair2D) float64 {
	x := p.X - from.X
	y := p.Y - from.Y
	return math.Sqrt(float64(x*x) + float64(y*y))
}

func main() {
	var h Hoger = HogeAdapter(FugaExec)
	TestExec(h, "test")

	data := `name,age,has_pet
Jon,100,true
Fred The Hnmmer Smith,42,false
Martha,37,true`

	r := csv.NewReader(strings.NewReader(data))
	allData, err := r.ReadAll()
	if err != nil {
		panic(err)
	}
	fmt.Println(allData)
	var entries []MyData
	csvconvert.Unmarshal(allData, &entries)
	fmt.Println(entries)

	out, err := csvconvert.Marshal(entries)
	if err != nil {
		panic(err)
	}
	sb := &strings.Builder{}
	w := csv.NewWriter(sb)
	w.WriteAll(out)
	fmt.Println(sb)

	words := []string{"one", "poteto", "two", "poteto", "three"}
	filterd := Filter(words, func(s string) bool {
		return s != "poteto"
	})
	fmt.Println(filterd)
	lengthes := Map(filterd, func(s string) int {
		return len(s)
	})
	fmt.Println(lengthes)

	pair2Da := Pair[Pair2D]{Pair2D{X: 5, Y: 10}, Pair2D{X: 10, Y: 20}}
	pair2Db := Pair[Pair2D]{Pair2D{X: 15, Y: 20}, Pair2D{X: 30, Y: 40}}
	closer := FindCloser(pair2Da, pair2Db)
	fmt.Println(closer)
}
