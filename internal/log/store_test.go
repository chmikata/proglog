package log

import (
	"bufio"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_newStore(t *testing.T) {
	t.Parallel()

	type args struct {
		f *os.File
	}
	f, _ := os.CreateTemp("", "Test_newStore")
	t.Cleanup(func() { os.Remove(f.Name()) })
	fi, _ := os.Stat(f.Name())
	s := store{
		file: f,
		size: uint64(fi.Size()),
		buf:  bufio.NewWriter(f),
	}
	tests := []struct {
		name      string
		args      args
		want      *store
		assertion assert.ErrorAssertionFunc
	}{
		{
			name: "ok case",
			args: args{f: f},
			want: &s,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(tt, err)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := newStore(tt.args.f)
			tt.assertion(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_store_Append(t *testing.T) {
	t.Parallel()

	type args struct {
		p []byte
	}
	f, _ := os.CreateTemp("", "Test_store_Append")
	t.Cleanup(func() { os.Remove(f.Name()) })
	st, _ := newStore(f)
	tests := []struct {
		name      string
		store     *store
		args      args
		want      uint64
		want1     uint64
		assertion assert.ErrorAssertionFunc
	}{
		{
			name:  "ok case 1",
			store: st,
			args:  args{[]byte("test1")},
			want:  13,
			want1: 0,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(tt, err)
			},
		},
		{
			name:  "ok case 2",
			store: st,
			args:  args{[]byte("test2-hoge")},
			want:  18,
			want1: 13,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(tt, err)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := tt.store.Append(tt.args.p)
			tt.assertion(t, err)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.want1, got1)
		})
	}
}

func Test_store_Read(t *testing.T) {
	t.Parallel()

	type args struct {
		pos uint64
	}
	f, _ := os.CreateTemp("", "Test_store_Read")
	t.Cleanup(func() { os.Remove(f.Name()) })
	st, _ := newStore(f)
	tests := []struct {
		name      string
		store     *store
		args      args
		want      []byte
		assertion assert.ErrorAssertionFunc
	}{
		{
			name:  "ok case",
			store: st,
			args:  args{pos: 0},
			want:  []byte("test"),
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(tt, err)
			},
		},
		{
			name:  "pos over",
			store: st,
			args:  args{pos: 5},
			want:  nil,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				return assert.Error(tt, err)
			},
		},
		{
			name:  "pos+lenWith over",
			store: st,
			args:  args{pos: 1},
			want:  nil,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				return assert.Error(tt, err)
			},
		},
	}

	st.Append([]byte("test"))
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.store.Read(tt.args.pos)
			tt.assertion(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_store_ReadAt(t *testing.T) {
	t.Parallel()

	type args struct {
		p   []byte
		off int64
	}
	f, _ := os.CreateTemp("", "Test_store_ReadAt")
	t.Cleanup(func() { os.Remove(f.Name()) })
	st, _ := newStore(f)
	tests := []struct {
		name      string
		store     *store
		args      args
		want      int
		assertion assert.ErrorAssertionFunc
	}{
		{
			name:  "ok case1",
			store: st,
			args:  args{p: make([]byte, lenWidth), off: 0},
			want:  8,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(tt, err)
			},
		},
		{
			name:  "ok case2",
			store: st,
			args:  args{p: make([]byte, 4), off: 8},
			want:  4,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				if assert.NoError(tt, err) {
					return assert.Equal(tt, []byte("test"), i[0].([]byte))
				}
				return false
			},
		},
	}

	st.Append([]byte("test"))
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.store.ReadAt(tt.args.p, tt.args.off)
			tt.assertion(t, err, tt.args.p)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_store_Close(t *testing.T) {
	t.Parallel()

	f, _ := os.CreateTemp("", "Test_store_Close")
	t.Cleanup(func() { os.Remove(f.Name()) })
	st, _ := newStore(f)
	tests := []struct {
		name      string
		store     *store
		assertion assert.ErrorAssertionFunc
	}{
		{
			name:  "ok case",
			store: st,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(tt, err)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.store.Append([]byte("test"))
			fi, _ := os.Stat(f.Name())
			b_size := fi.Size()
			tt.assertion(t, tt.store.Close())
			fi, _ = os.Stat(f.Name())
			a_size := fi.Size()
			assert.Greater(t, a_size, b_size)
		})
	}
}

func Test_store_Name(t *testing.T) {
	t.Parallel()

	f, _ := os.CreateTemp("", "Test_store_Name")
	t.Cleanup(func() { os.Remove(f.Name()) })
	st, _ := newStore(f)
	tests := []struct {
		name  string
		store *store
		want  string
	}{
		{
			name:  "ok case",
			store: st,
			want:  f.Name(),
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.store.Name())
		})
	}
}
