package sarama

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"sync"
	"time"

	snappy "github.com/eapache/go-xerial-snappy"
	"github.com/pierrec/lz4"
)

var (
	lz4ReaderPool = sync.Pool{
		New: func() interface{} {
			return lz4.NewReader(nil)
		},
	}

	gzipReaderPool sync.Pool
	bufferPool     = sync.Pool{
		New: func() interface{} {
			return newByteBuffer(newBufDefaultInitSize)
		},
	}
)

func decompress(cc CompressionCodec, data []byte) ([]byte, error) {
	switch cc {
	case CompressionNone:
		return data, nil
	case CompressionGZIP:
		// decompressionRatio somewhat high de-compression ratio, to ensure enough buffer is allocated upfront
		//const decompressionRatio = 5
		var (
			err        error
			reader     *gzip.Reader
			readerIntf = gzipReaderPool.Get()
		)
		if readerIntf != nil {
			reader = readerIntf.(*gzip.Reader)
		} else {
			reader, err = gzip.NewReader(bytes.NewReader(data))
			if err != nil {
				return nil, err
			}
		}

		defer gzipReaderPool.Put(reader)

		buffer := bufferPool.Get().(*byteBuffer)
		defer bufferPool.Put(buffer)

		if err := reader.Reset(bytes.NewReader(data)); err != nil {
			return nil, err
		}
		_, err = io.Copy(buffer, reader)
		return buffer.Next(), err
	case CompressionSnappy:
		return snappy.Decode(data)
	case CompressionLZ4:
		reader := lz4ReaderPool.Get().(*lz4.Reader)
		defer lz4ReaderPool.Put(reader)

		reader.Reset(bytes.NewReader(data))
		return ioutil.ReadAll(reader)
	case CompressionZSTD:
		return zstdDecompress(nil, data)
	default:
		return nil, PacketDecodingError{fmt.Sprintf("invalid compression specified (%d)", cc)}
	}
}

const (
	newBufDefaultInitSize = 2 * 1024 * 1024
)

type byteBuffer struct {
	buf        []byte
	newBufSize int
}

func newByteBuffer(newBufSize int) *byteBuffer {
	return &byteBuffer{
		newBufSize: newBufSize,
	}
}

func (b *byteBuffer) Write(src []byte) (n int, err error) {
	srcSize := len(src)
	if b.buf == nil {
		b.buf = make([]byte, srcSize, srcSize+b.newBufSize)
		return copy(b.buf, src), nil
	}
	bufInitSize := len(b.buf)
	if srcSize <= cap(b.buf)-bufInitSize {
		b.buf = b.buf[:bufInitSize+srcSize]
		return copy(b.buf[bufInitSize:], src), nil
	}
	newBuf := make([]byte, bufInitSize+srcSize, bufInitSize+srcSize+b.newBufSize)
	copy(newBuf, b.buf)
	b.buf = newBuf
	return copy(newBuf[bufInitSize:], src), nil
}

func (b *byteBuffer) ReadFrom(r io.Reader) (n int64, err error) {
	// Init buffer if needed
	if b.buf == nil {
		b.buf = make([]byte, 0, b.newBufSize)
	}
	sleep := time.Microsecond
	for {
		// If the buffer is full, make a copy with extra capacity
		if len(b.buf) == cap(b.buf) {
			newBuf := make([]byte, len(b.buf), len(b.buf)+b.newBufSize)
			copy(newBuf, b.buf)
			b.buf = newBuf
		}

		// Read into the buffer remaining space/capacity
		var nRead int
		nRead, err = r.Read(b.buf[len(b.buf):cap(b.buf)])
		n = n + int64(nRead)
		b.buf = b.buf[:len(b.buf)+nRead]
		if err == io.EOF {
			return n, nil
		} else if err != nil {
			return n, err
		}
		if nRead == 0 && err == nil {
			// Nothing happened, incrementally slowdown retries
			time.Sleep(sleep)
			sleep = 2 * sleep
		} else {
			sleep = time.Microsecond
		}
	}
}

func (b *byteBuffer) Next() (res []byte) {
	res = b.buf
	b.buf = b.buf[len(b.buf):]
	return res
}
