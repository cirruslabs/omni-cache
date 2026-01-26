package urlproxy

import "io"

type countingReader struct {
	reader io.Reader
	bytes  int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.bytes += int64(n)
	return n, err
}

func (r *countingReader) Bytes() int64 {
	return r.bytes
}
