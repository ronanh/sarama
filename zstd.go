package sarama

import (
	"bytes"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
)

var (
	zstdReaderPool *CloserPool = NewCloserPool(16, nil)
	zstdWriterPool *CloserPool = NewCloserPool(16, nil)

	bufferPool sync.Pool = sync.Pool{
		New: func() interface{} { return &bytes.Buffer{} },
	}
)

func zstdDecompress(src []byte) ([]byte, error) {
	zstdDec, err := getZstdReader(src)
	if err != nil {
		return nil, err
	}
	defer zstdReaderPool.Put(zstdDec)

	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	if _, err := zstdDec.WriteTo(buf); err != nil {
		return nil, err
	}

	// Return a copy of the work buffer
	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out, nil
}

func getZstdReader(src []byte) (*zstd.Decoder, error) {
	srcBuf := bytes.NewBuffer(src)
	if r := zstdReaderPool.Get(); r != nil {
		zstdDec := r.(*zstd.Decoder)
		if err := zstdDec.Reset(srcBuf); err != nil {
			zstdReaderPool.Put(zstdDec)
			return nil, err
		}
		return zstdDec, nil
	}
	return zstd.NewReader(srcBuf)
}

func zstdCompress(src []byte) ([]byte, error) {
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	zstdEnc, err := getZstdWriter(buf)
	if err != nil {
		return nil, err
	}

	if _, err := zstdEnc.Write(src); err != nil {
		return nil, err
	}
	if err := zstdEnc.Flush(); err != nil {
		return nil, err
	}

	// Return a copy of the work buffer
	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out, nil
}

func getZstdWriter(w io.Writer) (*zstd.Encoder, error) {
	if r := zstdWriterPool.Get(); r != nil {
		zstdEnc := r.(*zstd.Encoder)
		zstdEnc.Reset(w)
		return zstdEnc, nil
	}
	return zstd.NewWriter(w, zstd.WithZeroFrames(true))
}
