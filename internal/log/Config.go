package log

type Config struct {
	Segment struct {
		MaxStoreBytes uint64
		MaxindexBytes uint64
		InitialOffset uint64
	}
}
