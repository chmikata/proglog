package log

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tysonmote/gommap"
)

func Test_newIndex(t *testing.T) {
	t.Parallel()

	f, _ := os.CreateTemp("", "Test_newIndex")
	t.Cleanup(func() { os.Remove(f.Name()) })
	c := Config{
		Segment: struct {
			MaxStoreBytes uint64
			MaxIndexBytes uint64
			InitialOffset uint64
		}{
			MaxStoreBytes: 100,
			MaxIndexBytes: 100,
			InitialOffset: 0,
		},
	}
	idx := index{
		file: f,
	}
	os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes))
	fi, _ := os.Stat(f.Name())
	idx.size = uint64(fi.Size())
	idx.mmap, _ = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	)
	type args struct {
		f *os.File
		c Config
	}
	tests := []struct {
		name      string
		args      args
		want      *index
		assertion assert.ErrorAssertionFunc
	}{
		{
			name: "ok case",
			args: args{f: f, c: c},
			want: &idx,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(tt, err)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := newIndex(tt.args.f, tt.args.c)
			tt.assertion(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_index_Close(t *testing.T) {
	f, _ := os.CreateTemp("", "Test_index_Close")
	t.Cleanup(func() { os.Remove(f.Name()) })
	c := Config{
		Segment: struct {
			MaxStoreBytes uint64
			MaxIndexBytes uint64
			InitialOffset uint64
		}{
			MaxStoreBytes: 100,
			MaxIndexBytes: 100,
			InitialOffset: 0,
		},
	}
	idx, _ := newIndex(f, c)
	tests := []struct {
		name      string
		index     *index
		assertion assert.ErrorAssertionFunc
	}{
		{
			name:  "ok case",
			index: idx,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(tt, err)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.index.Write(10, 0)
			fi, _ := os.Stat(f.Name())
			b_size := fi.Size()
			tt.assertion(t, tt.index.Close())
			fi, _ = os.Stat(f.Name())
			a_size := fi.Size()
			assert.Less(t, a_size, b_size)
		})
	}
}

func Test_index_Read(t *testing.T) {
	f, _ := os.CreateTemp("", "Test_index_Read")
	t.Cleanup(func() { os.Remove(f.Name()) })
	c := Config{
		Segment: struct {
			MaxStoreBytes uint64
			MaxIndexBytes uint64
			InitialOffset uint64
		}{
			MaxStoreBytes: 100,
			MaxIndexBytes: 100,
			InitialOffset: 0,
		},
	}
	idx, _ := newIndex(f, c)
	type args struct {
		in int64
	}
	type test struct {
		name      string
		index     *index
		args      args
		want      uint32
		want1     uint64
		assertion assert.ErrorAssertionFunc
		setup     func(tst *test)
	}
	tests := []test{
		{
			name:  "ok size 0 case",
			index: idx,
			args:  args{in: -1},
			want:  0,
			want1: 0,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				if assert.Error(tt, err) {
					return assert.Equal(tt, io.EOF, err)
				}
				return false
			},
			setup: func(tst *test) {},
		},
		{
			name:  "ok param -1 case",
			index: idx,
			args:  args{in: -1},
			want:  0,
			want1: 10,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(tt, err)
			},
			setup: func(tst *test) {
				tst.index.Write(0, 10)
			},
		},
		{
			name:  "ok param 1 case",
			index: idx,
			args:  args{in: 1},
			want:  1,
			want1: 20,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(tt, err)
			},
			setup: func(tst *test) {
				tst.index.Write(1, 20)
			},
		},
		{
			name:  "error over pos case",
			index: idx,
			args:  args{in: 2},
			want:  0,
			want1: 0,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				if assert.Error(tt, err) {
					return assert.Equal(tt, io.EOF, err)
				}
				return false
			},
			setup: func(tst *test) {},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			tt.setup(&tt)
			got, got1, err := tt.index.Read(tt.args.in)
			tt.assertion(t, err)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.want1, got1)
		})
	}
}

func Test_index_Write(t *testing.T) {
	f, _ := os.CreateTemp("", "Test_index_Write")
	t.Cleanup(func() { os.Remove(f.Name()) })
	c := Config{
		Segment: struct {
			MaxStoreBytes uint64
			MaxIndexBytes uint64
			InitialOffset uint64
		}{
			MaxStoreBytes: 100,
			MaxIndexBytes: 12,
			InitialOffset: 0,
		},
	}
	idx, _ := newIndex(f, c)
	type args struct {
		off uint32
		pos uint64
	}
	tests := []struct {
		name      string
		index     *index
		args      args
		assertion assert.ErrorAssertionFunc
	}{
		{
			name:  "ok case",
			index: idx,
			args:  args{off: 0, pos: 10},
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(tt, err)
			},
		},
		{
			name:  "error over limit case",
			index: idx,
			args:  args{off: 1, pos: 20},
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				if assert.Error(tt, err) {
					return assert.Equal(tt, io.EOF, err)
				}
				return false
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			tt.assertion(t, tt.index.Write(tt.args.off, tt.args.pos))
		})
	}
}

func Test_index_isMaxed(t *testing.T) {
	f, _ := os.CreateTemp("", "Test_index_isMaxed")
	t.Cleanup(func() { os.Remove(f.Name()) })
	c := Config{
		Segment: struct {
			MaxStoreBytes uint64
			MaxIndexBytes uint64
			InitialOffset uint64
		}{
			MaxStoreBytes: 100,
			MaxIndexBytes: 12,
			InitialOffset: 0,
		},
	}
	idx, _ := newIndex(f, c)
	type test struct {
		name  string
		index *index
		want  bool
		setup func(tst *test)
	}
	tests := []test{
		{
			name:  "false less max case",
			index: idx,
			want:  false,
			setup: func(tst *test) {},
		},
		{
			name:  "true over max case",
			index: idx,
			want:  true,
			setup: func(tst *test) {
				tst.index.size = 12
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			tt.setup(&tt)
			assert.Equal(t, tt.want, tt.index.isMaxed())
		})
	}
}

func Test_index_Name(t *testing.T) {
	f, _ := os.CreateTemp("", "Test_index_Name")
	t.Cleanup(func() { os.Remove(f.Name()) })
	c := Config{
		Segment: struct {
			MaxStoreBytes uint64
			MaxIndexBytes uint64
			InitialOffset uint64
		}{
			MaxStoreBytes: 100,
			MaxIndexBytes: 12,
			InitialOffset: 0,
		},
	}
	idx, _ := newIndex(f, c)
	tests := []struct {
		name  string
		index *index
		want  string
	}{
		{
			name:  "ok case",
			index: idx,
			want:  f.Name(),
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.index.Name())
		})
	}
}
