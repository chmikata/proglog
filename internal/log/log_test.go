package log

import (
	"fmt"
	"io"
	"os"
	"sync"
	"testing"

	api "github.com/chmikata/proglog/api/v1"
	"github.com/stretchr/testify/assert"
)

func TestNewLog(t *testing.T) {
	dir := "/tmp/log_store"
	t.Cleanup(func() {
		os.Remove(fmt.Sprintf("%s/0.store", dir))
		os.Remove(fmt.Sprintf("%s/0.index", dir))
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
	type args struct {
		dir string
		c   Config
	}
	tests := []struct {
		name      string
		args      args
		assertion assert.ErrorAssertionFunc
	}{
		{
			name: "ok case",
			args: args{dir: dir, c: c},
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(tt, err)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewLog(tt.args.dir, tt.args.c)
			tt.assertion(t, err)
			assert.Equal(t, 1, len(got.segments))
			assert.Equal(t, uint64(0), got.activeSegment.baseOffset)
			assert.Equal(t, uint64(0), got.activeSegment.nextOffset)
			assert.Equal(t, dir, got.Dir)
			assert.Equal(t, c, got.Config)
		})
	}
}

func TestLog_setup(t *testing.T) {
	dir := "/tmp/log_store"
	t.Cleanup(func() {
		os.Remove(fmt.Sprintf("%s/0.store", dir))
		os.Remove(fmt.Sprintf("%s/0.index", dir))
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
	type fields struct {
		Dir    string
		Config Config
	}
	tests := []struct {
		name      string
		fields    fields
		assertion assert.ErrorAssertionFunc
	}{
		{
			name: "ok case",
			fields: fields{
				Dir:    dir,
				Config: c,
			},
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(tt, err)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			l := &Log{
				Dir:    tt.fields.Dir,
				Config: tt.fields.Config,
			}
			tt.assertion(t, l.setup(), l)
			assert.Equal(t, uint64(0), l.activeSegment.baseOffset)
			assert.Equal(t, uint64(0), l.activeSegment.nextOffset)
			assert.Equal(t, 1, len(l.segments))
		})
	}
}

func TestLog_Append(t *testing.T) {
	dir := "/tmp/log_store"
	t.Cleanup(func() {
		os.Remove(fmt.Sprintf("%s/0.store", dir))
		os.Remove(fmt.Sprintf("%s/0.index", dir))
	})
	c := Config{
		Segment: struct {
			MaxStoreBytes uint64
			MaxindexBytes uint64
			InitialOffset uint64
		}{
			MaxStoreBytes: 128,
			MaxindexBytes: 128,
			InitialOffset: 0,
		},
	}
	log, _ := NewLog(dir, c)
	type args struct {
		record *api.Record
	}
	tests := []struct {
		name      string
		log       *Log
		args      args
		want      uint64
		assertion assert.ErrorAssertionFunc
	}{
		{
			name: "ok case first append",
			log:  log,
			args: args{record: &api.Record{Value: []byte("test")}},
			want: 0,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(tt, err)
			},
		},
		{
			name: "ok case second append",
			log:  log,
			args: args{record: &api.Record{Value: []byte("test")}},
			want: 1,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(tt, err)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.log.Append(tt.args.record)
			tt.assertion(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLog_Read(t *testing.T) {
	dir := "/tmp/log_store"
	t.Cleanup(func() {
		os.Remove(fmt.Sprintf("%s/0.store", dir))
		os.Remove(fmt.Sprintf("%s/0.index", dir))
	})
	c := Config{
		Segment: struct {
			MaxStoreBytes uint64
			MaxindexBytes uint64
			InitialOffset uint64
		}{
			MaxStoreBytes: 128,
			MaxindexBytes: 128,
			InitialOffset: 0,
		},
	}
	log, _ := NewLog(dir, c)
	type args struct {
		off uint64
	}
	tests := []struct {
		name      string
		log       *Log
		args      args
		want      *api.Record
		assertion assert.ErrorAssertionFunc
		setup     func(*Log, args)
	}{
		{
			name: "ok case 0 index read",
			log:  log,
			args: args{off: 0},
			want: &api.Record{
				Value:  []byte("test"),
				Offset: 0,
			},
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				want := i[0].(*api.Record)
				got := i[1].(*api.Record)
				assert.Equal(t, want.Value, got.Value)
				assert.Equal(t, want.Offset, got.Offset)
				return assert.NoError(tt, err)
			},
			setup: func(l *Log, _ args) {
				l.Append(&api.Record{
					Value: []byte("test"),
				})
			},
		},
		{
			name: "ok case 1 index read",
			log:  log,
			args: args{off: 1},
			want: &api.Record{
				Value:  []byte("test"),
				Offset: 1,
			},
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				want := i[0].(*api.Record)
				got := i[1].(*api.Record)
				assert.Equal(t, want.Value, got.Value)
				assert.Equal(t, want.Offset, got.Offset)
				return assert.NoError(tt, err)
			},
			setup: func(l *Log, _ args) {
				l.Append(&api.Record{
					Value: []byte("test"),
				})
			},
		},
		{
			name: "error case offset out range",
			log:  log,
			args: args{off: 2},
			want: nil,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				want := i[0].(*api.Record)
				got := i[1].(*api.Record)
				assert.Equal(t, want, got)
				assert.IsType(t, api.ErrOffsetOutOfRange{}, err)
				return assert.Error(tt, err)
			},
			setup: func(_ *Log, _ args) {},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			tt.setup(tt.log, tt.args)
			got, err := tt.log.Read(tt.args.off)
			tt.assertion(t, err, tt.want, got)
		})
	}
}

func TestLog_LowestOffset(t *testing.T) {
	dir := "/tmp/log_store"
	t.Cleanup(func() {
		os.Remove(fmt.Sprintf("%s/10.store", dir))
		os.Remove(fmt.Sprintf("%s/10.index", dir))
	})
	c := Config{
		Segment: struct {
			MaxStoreBytes uint64
			MaxindexBytes uint64
			InitialOffset uint64
		}{
			MaxStoreBytes: 128,
			MaxindexBytes: 128,
			InitialOffset: 10,
		},
	}
	log, _ := NewLog(dir, c)
	tests := []struct {
		name      string
		log       *Log
		want      uint64
		assertion assert.ErrorAssertionFunc
	}{
		{
			name: "ok case",
			log:  log,
			want: 10,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(tt, err)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.log.LowestOffset()
			tt.assertion(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLog_HighestOffset(t *testing.T) {
	dir := "/tmp/log_store"
	t.Cleanup(func() {
		os.Remove(fmt.Sprintf("%s/10.store", dir))
		os.Remove(fmt.Sprintf("%s/10.index", dir))
	})
	c := Config{
		Segment: struct {
			MaxStoreBytes uint64
			MaxindexBytes uint64
			InitialOffset uint64
		}{
			MaxStoreBytes: 128,
			MaxindexBytes: 128,
			InitialOffset: 10,
		},
	}
	log, _ := NewLog(dir, c)
	tests := []struct {
		name      string
		log       *Log
		want      uint64
		assertion assert.ErrorAssertionFunc
		setup     func(*Log)
	}{
		{
			name: "ok case initial",
			log:  log,
			want: 9,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(tt, err)
			},
			setup: func(_ *Log) {},
		},
		{
			name: "ok case 1 append",
			log:  log,
			want: 10,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(tt, err)
			},
			setup: func(l *Log) {
				l.Append(&api.Record{
					Value: []byte("test"),
				})
			},
		},
		{
			name: "ok case 2 append",
			log:  log,
			want: 11,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(tt, err)
			},
			setup: func(l *Log) {
				l.Append(&api.Record{
					Value: []byte("test"),
				})
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			tt.setup(tt.log)
			got, err := tt.log.HighestOffset()
			tt.assertion(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLog_newSegment(t *testing.T) {
	dir := "/tmp/log_store"
	t.Cleanup(func() {
		os.Remove(fmt.Sprintf("%s/20.store", dir))
		os.Remove(fmt.Sprintf("%s/20.index", dir))
	})
	type fields struct {
		Dir           string
		Config        Config
		activeSegment *segment
		segments      []*segment
	}
	type args struct {
		off uint64
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		assertion assert.ErrorAssertionFunc
	}{
		{
			name: "ok case",
			fields: fields{
				Dir: dir,
				Config: Config{
					Segment: struct {
						MaxStoreBytes uint64
						MaxindexBytes uint64
						InitialOffset uint64
					}{
						MaxStoreBytes: 128,
						MaxindexBytes: 128,
						InitialOffset: 20,
					},
				},
				activeSegment: nil,
				segments:      []*segment{},
			},
			args: args{off: 20},
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				l := i[0].(*Log)
				assert.Equal(tt, 1, len(l.segments))
				assert.Equal(tt, l.segments[0], l.activeSegment)
				assert.Equal(tt, uint64(20), l.activeSegment.baseOffset)
				assert.Equal(tt, uint64(20), l.activeSegment.nextOffset)
				return assert.NoError(tt, err)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			l := &Log{
				mu:            sync.RWMutex{},
				Dir:           tt.fields.Dir,
				Config:        tt.fields.Config,
				activeSegment: tt.fields.activeSegment,
				segments:      tt.fields.segments,
			}
			tt.assertion(t, l.newSegment(tt.args.off), l)
		})
	}
}

func TestLog_Close(t *testing.T) {
	dir := "/tmp/log_store"
	t.Cleanup(func() {
		os.Remove(fmt.Sprintf("%s/100.store", dir))
		os.Remove(fmt.Sprintf("%s/100.index", dir))
		os.Remove(fmt.Sprintf("%s/103.store", dir))
		os.Remove(fmt.Sprintf("%s/103.index", dir))
	})
	c := Config{
		Segment: struct {
			MaxStoreBytes uint64
			MaxindexBytes uint64
			InitialOffset uint64
		}{
			MaxStoreBytes: 120,
			MaxindexBytes: 36,
			InitialOffset: 100,
		},
	}
	log, _ := NewLog(dir, c)
	tests := []struct {
		name      string
		log       *Log
		assertion assert.ErrorAssertionFunc
		setup     func(*Log)
	}{
		{
			name: "ok case",
			log:  log,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				fi, _ := os.Stat(fmt.Sprintf("%s/100.store", dir))
				assert.Equal(tt, int64(48), fi.Size())
				fi, _ = os.Stat(fmt.Sprintf("%s/103.store", dir))
				assert.Equal(tt, int64(48), fi.Size())
				fi, _ = os.Stat(fmt.Sprintf("%s/100.index", dir))
				assert.Equal(tt, int64(36), fi.Size())
				fi, _ = os.Stat(fmt.Sprintf("%s/103.index", dir))
				assert.Equal(tt, int64(36), fi.Size())
				return assert.NoError(tt, err)
			},
			setup: func(l *Log) {
				for i := 0; i < 6; i++ {
					l.Append(&api.Record{Value: []byte("test")})
				}
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			tt.setup(tt.log)
			tt.assertion(t, tt.log.Close())
		})
	}
}

func TestLog_Remove(t *testing.T) {
	dir := "/tmp/log_store"
	t.Cleanup(func() {
		os.Remove(fmt.Sprintf("%s/110.store", dir))
		os.Remove(fmt.Sprintf("%s/110.index", dir))
	})
	c := Config{
		Segment: struct {
			MaxStoreBytes uint64
			MaxindexBytes uint64
			InitialOffset uint64
		}{
			MaxStoreBytes: 120,
			MaxindexBytes: 120,
			InitialOffset: 110,
		},
	}
	log, _ := NewLog(dir, c)
	tests := []struct {
		name      string
		log       *Log
		assertion assert.ErrorAssertionFunc
	}{
		{
			name: "ok case",
			log:  log,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				assert.NoFileExists(tt, fmt.Sprintf("%s/110.store", dir))
				assert.NoFileExists(tt, fmt.Sprintf("%s/110.index", dir))
				return assert.NoError(tt, err)
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			tt.assertion(t, tt.log.Remove())
		})
	}
}

func TestLog_Reset(t *testing.T) {
	dir := "/tmp/log_store"
	t.Cleanup(func() {
		os.Remove(fmt.Sprintf("%s/120.store", dir))
		os.Remove(fmt.Sprintf("%s/120.index", dir))
	})
	c := Config{
		Segment: struct {
			MaxStoreBytes uint64
			MaxindexBytes uint64
			InitialOffset uint64
		}{
			MaxStoreBytes: 120,
			MaxindexBytes: 120,
			InitialOffset: 120,
		},
	}
	log, _ := NewLog(dir, c)
	tests := []struct {
		name      string
		log       *Log
		assertion assert.ErrorAssertionFunc
		setup     func(*Log)
	}{
		{
			name: "ok case",
			log:  log,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				fi, _ := os.Stat(fmt.Sprintf("%s/120.store", dir))
				assert.Equal(tt, int64(0), fi.Size())
				assert.NoFileExists(tt, fmt.Sprintf("%s/128.store", dir))
				fi, _ = os.Stat(fmt.Sprintf("%s/120.index", dir))
				assert.Equal(tt, int64(120), fi.Size())
				assert.NoFileExists(tt, fmt.Sprintf("%s/128.index", dir))
				return assert.NoError(tt, err)
			},
			setup: func(l *Log) {
				for i := 0; i < 10; i++ {
					l.Append(&api.Record{Value: []byte("test")})
				}
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			tt.setup(tt.log)
			tt.assertion(t, tt.log.Reset())
		})
	}
}

func TestLog_Truncate(t *testing.T) {
	dir := "/tmp/log_store"
	t.Cleanup(func() {
		os.Remove(fmt.Sprintf("%s/130.store", dir))
		os.Remove(fmt.Sprintf("%s/130.index", dir))
		os.Remove(fmt.Sprintf("%s/138.store", dir))
		os.Remove(fmt.Sprintf("%s/138.index", dir))
	})
	c := Config{
		Segment: struct {
			MaxStoreBytes uint64
			MaxindexBytes uint64
			InitialOffset uint64
		}{
			MaxStoreBytes: 120,
			MaxindexBytes: 120,
			InitialOffset: 130,
		},
	}
	log, _ := NewLog(dir, c)
	type args struct {
		lowest uint64
	}
	tests := []struct {
		name      string
		log       *Log
		args      args
		assertion assert.ErrorAssertionFunc
		setup     func(*Log)
	}{
		{
			name: "ok case",
			log:  log,
			args: args{lowest: 138},
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				assert.NoFileExists(tt, fmt.Sprintf("%s/130.store", dir))
				assert.NoFileExists(tt, fmt.Sprintf("%s/130.index", dir))
				assert.FileExists(tt, fmt.Sprintf("%s/138.store", dir))
				assert.FileExists(tt, fmt.Sprintf("%s/138.index", dir))
				return assert.NoError(tt, err)
			},
			setup: func(l *Log) {
				for i := 0; i < 10; i++ {
					l.Append(&api.Record{Value: []byte("test")})
				}
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			tt.setup(tt.log)
			tt.assertion(t, tt.log.Truncate(tt.args.lowest))
		})
	}
}

func TestLog_Reader(t *testing.T) {
	dir := "/tmp/log_store"
	t.Cleanup(func() {
		os.Remove(fmt.Sprintf("%s/140.store", dir))
		os.Remove(fmt.Sprintf("%s/140.index", dir))
	})
	c := Config{
		Segment: struct {
			MaxStoreBytes uint64
			MaxindexBytes uint64
			InitialOffset uint64
		}{
			MaxStoreBytes: 120,
			MaxindexBytes: 120,
			InitialOffset: 140,
		},
	}
	log, _ := NewLog(dir, c)
	tests := []struct {
		name string
		log  *Log
		want io.Reader
	}{
		{
			name: "ok case",
			log:  log,
			want: io.MultiReader(&originReader{
				s:   log.segments[0].store,
				off: 0,
			}),
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.log.Reader())
		})
	}
}

func Test_originReader_Read(t *testing.T) {
	dir := "/tmp/log_store"
	t.Cleanup(func() {
		os.Remove(fmt.Sprintf("%s/150.store", dir))
	})
	f, _ := os.OpenFile(
		fmt.Sprintf("%s/150.store", dir),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0600,
	)
	s, _ := newStore(f)
	type fields struct {
		s   *store
		off int64
	}
	type args struct {
		p []byte
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		want      int
		assertion assert.ErrorAssertionFunc
		setup     func(*store)
	}{
		{
			name: "ok case",
			fields: fields{
				s:   s,
				off: 0,
			},
			args: args{
				p: make([]byte, 20),
			},
			want: 20,
			assertion: func(tt assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(tt, err)
			},
			setup: func(s *store) {
				p := []byte("testhogefuga")
				s.Append(p)
				s.buf.Flush()
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			tt.setup(tt.fields.s)
			o := &originReader{
				s:   tt.fields.s,
				off: tt.fields.off,
			}
			got, err := o.Read(tt.args.p)
			tt.assertion(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
