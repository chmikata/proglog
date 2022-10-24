package log

import (
	"fmt"
	"io"
	"os"
	"testing"

	api "github.com/chmikata/proglog/api/v1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func Test_newSegment(t *testing.T) {
	t.Parallel()

	dir := "/tmp"
	baseOffset := 0
	fs, _ := os.CreateTemp("", "Test_newSegment_store")
	fi, _ := os.CreateTemp("", "Test_newSegment_index")
	t.Cleanup(func() {
		os.Remove(fs.Name())
		os.Remove(fi.Name())
		os.Remove(fmt.Sprintf("%s/%d.store", dir, baseOffset))
		os.Remove(fmt.Sprintf("%s/%d.index", dir, baseOffset))
	})
	c := Config{
		Segment: struct {
			MaxStoreBytes uint64
			MaxindexBytes uint64
			InitialOffset uint64
		}{
			MaxStoreBytes: 100,
			MaxindexBytes: 100,
			InitialOffset: 0,
		},
	}
	st, _ := newStore(fs)
	idx, _ := newIndex(fi, c)
	seg := segment{
		store:      st,
		index:      idx,
		baseOffset: 0,
		nextOffset: 0,
		config:     c,
	}
	type args struct {
		dir        string
		baseOffset uint64
		c          Config
	}
	tests := []struct {
		name      string
		args      args
		want      *segment
		assertion assert.ErrorAssertionFunc
	}{
		{
			name: "ok case",
			args: args{
				dir:        dir,
				baseOffset: uint64(baseOffset),
				c:          c,
			},
			want: &seg,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(tt, err)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := newSegment(tt.args.dir, tt.args.baseOffset, tt.args.c)
			tt.assertion(t, err)
			assert.Equal(t, tt.want.baseOffset, got.baseOffset)
			assert.Equal(t, tt.want.nextOffset, got.baseOffset)
			assert.Equal(t, tt.want.config, got.config)
		})
	}
}

func Test_segment_Append(t *testing.T) {
	t.Parallel()

	dir := "/tmp"
	baseOffset := 10
	t.Cleanup(func() {
		os.Remove(fmt.Sprintf("%s/%d.store", dir, baseOffset))
		os.Remove(fmt.Sprintf("%s/%d.index", dir, baseOffset))
	})
	c := Config{
		Segment: struct {
			MaxStoreBytes uint64
			MaxindexBytes uint64
			InitialOffset uint64
		}{
			MaxStoreBytes: 100,
			MaxindexBytes: 24,
			InitialOffset: 0,
		},
	}
	seg, _ := newSegment(dir, uint64(baseOffset), c)
	type args struct {
		record *api.Record
	}
	tests := []struct {
		name      string
		segment   *segment
		args      args
		want      uint64
		assertion assert.ErrorAssertionFunc
	}{
		{
			name:    "ok index 0 size case",
			segment: seg,
			args:    args{&api.Record{Value: []byte("hello world")}},
			want:    10,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(tt, err)
			},
		},
		{
			name:    "ok index max size case",
			segment: seg,
			args:    args{&api.Record{Value: []byte("hello world")}},
			want:    11,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(tt, err)
			},
		},
		{
			name:    "error index over case",
			segment: seg,
			args:    args{&api.Record{Value: []byte("hoge fuga")}},
			want:    0,
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
			got, err := tt.segment.Append(tt.args.record)
			tt.assertion(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_segment_Read(t *testing.T) {
	t.Parallel()

	dir := "/tmp"
	baseOffset := 20
	t.Cleanup(func() {
		os.Remove(fmt.Sprintf("%s/%d.store", dir, baseOffset))
		os.Remove(fmt.Sprintf("%s/%d.index", dir, baseOffset))
	})
	c := Config{
		Segment: struct {
			MaxStoreBytes uint64
			MaxindexBytes uint64
			InitialOffset uint64
		}{
			MaxStoreBytes: 100,
			MaxindexBytes: 24,
			InitialOffset: 0,
		},
	}
	seg, _ := newSegment(dir, uint64(baseOffset), c)
	type args struct {
		off uint64
	}
	type test struct {
		name      string
		segment   *segment
		args      args
		want      *api.Record
		assertion assert.ErrorAssertionFunc
		setup     func(tst *test)
	}
	tests := []test{
		{
			name:    "ok case",
			segment: seg,
			args:    args{off: 20},
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(tt, err)
			},
			setup: func(tst *test) {
				rec := &api.Record{Value: []byte("hello world")}
				rec.Offset = tst.segment.nextOffset
				p, _ := proto.Marshal(rec)
				proto.Unmarshal(p, rec)
				tst.want = rec
				addrec := &api.Record{Value: []byte("hello world")}
				tst.segment.Append(addrec)
			},
		},
		{
			name:    "error index over case",
			segment: seg,
			args:    args{off: 22},
			want:    nil,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				if assert.Error(tt, err) {
					return assert.Equal(tt, io.EOF, err)
				}
				return false
			},
			setup: func(tst *test) {
				rec := &api.Record{Value: []byte("hello world")}
				tst.segment.Append(rec)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			tt.setup(&tt)
			got, err := tt.segment.Read(tt.args.off)
			tt.assertion(t, err)
			t.Log(tt.want)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_segment_IsMaxed(t *testing.T) {
	t.Parallel()

	dir := "/tmp"
	baseOffset := 30
	t.Cleanup(func() {
		os.Remove(fmt.Sprintf("%s/%d.store", dir, baseOffset))
		os.Remove(fmt.Sprintf("%s/%d.index", dir, baseOffset))
	})
	c := Config{
		Segment: struct {
			MaxStoreBytes uint64
			MaxindexBytes uint64
			InitialOffset uint64
		}{
			MaxStoreBytes: 100,
			MaxindexBytes: 12,
			InitialOffset: 0,
		},
	}
	seg, _ := newSegment(dir, uint64(baseOffset), c)
	type test struct {
		name    string
		segment *segment
		want    bool
		setup   func(tst *test)
	}
	tests := []test{
		{
			name:    "false less max case",
			segment: seg,
			want:    false,
			setup:   func(tst *test) {},
		},
		{
			name:    "true over max case",
			segment: seg,
			want:    true,
			setup: func(tst *test) {
				tst.segment.Append(&api.Record{Value: []byte("hoge fuga")})
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			tt.setup(&tt)
			assert.Equal(t, tt.want, tt.segment.IsMaxed())
		})
	}
}

func Test_segment_Remove(t *testing.T) {
	t.Parallel()

	dir := "/tmp"
	baseOffset := 40
	c := Config{
		Segment: struct {
			MaxStoreBytes uint64
			MaxindexBytes uint64
			InitialOffset uint64
		}{
			MaxStoreBytes: 100,
			MaxindexBytes: 12,
			InitialOffset: 0,
		},
	}
	seg, _ := newSegment(dir, uint64(baseOffset), c)
	type test struct {
		name      string
		segment   *segment
		assertion assert.ErrorAssertionFunc
		setup     func(tst *test)
	}
	tests := []test{
		{
			name:    "ok case",
			segment: seg,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(tt, err)
			},
			setup: func(tst *test) {
				tst.segment.Append(&api.Record{Value: []byte("test fuga")})
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.setup(&tt)
			tt.assertion(t, tt.segment.Remove())
			assert.NoFileExists(t, fmt.Sprintf("%s/%d.store", dir, baseOffset))
			assert.NoFileExists(t, fmt.Sprintf("%s/%d.index", dir, baseOffset))
		})
	}
}

func Test_segment_Close(t *testing.T) {
	t.Parallel()

	dir := "/tmp"
	baseOffset := 50
	t.Cleanup(func() {
		os.Remove(fmt.Sprintf("%s/%d.store", dir, baseOffset))
		os.Remove(fmt.Sprintf("%s/%d.index", dir, baseOffset))
	})
	c := Config{
		Segment: struct {
			MaxStoreBytes uint64
			MaxindexBytes uint64
			InitialOffset uint64
		}{
			MaxStoreBytes: 100,
			MaxindexBytes: 100,
			InitialOffset: 0,
		},
	}
	seg, _ := newSegment(dir, uint64(baseOffset), c)
	type test struct {
		name      string
		segment   *segment
		assertion assert.ErrorAssertionFunc
		setup     func(tst *test)
	}
	tests := []test{
		{
			name:    "ok case",
			segment: seg,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(tt, err)
			},
			setup: func(tst *test) {
				tst.segment.Append(&api.Record{Value: []byte("test hoge")})
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.setup(&tt)
			fi, _ := os.Stat(fmt.Sprintf("%s/%d.store", dir, baseOffset))
			b_store_size := fi.Size()
			fi, _ = os.Stat(fmt.Sprintf("%s/%d.index", dir, baseOffset))
			b_index_size := fi.Size()
			tt.assertion(t, tt.segment.Close())
			fi, _ = os.Stat(fmt.Sprintf("%s/%d.store", dir, baseOffset))
			a_store_size := fi.Size()
			fi, _ = os.Stat(fmt.Sprintf("%s/%d.index", dir, baseOffset))
			a_index_size := fi.Size()
			assert.Greater(t, a_store_size, b_store_size)
			assert.Less(t, a_index_size, b_index_size)
		})
	}
}
