package sarama

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"sync"

	snappy "github.com/eapache/go-xerial-snappy"
	"github.com/pierrec/lz4"
)

const (
	bufferSize = 1024 * 1024
)

var (
	lz4ReaderPool = sync.Pool{
		New: func() interface{} {
			return lz4.NewReader(nil)
		},
	}

	gzipReaderPool sync.Pool
	bufferPool     sync.Pool
)

func decompress(cc CompressionCodec, data []byte) ([]byte, error) {
	switch cc {
	case CompressionNone:
		return data, nil
	case CompressionGZIP:
		// decompressionRatio somewhat high de-compression ratio, to ensure enough buffer is allocated upfront
		const decompressionRatio = 5
		var (
			err        error
			reader     *gzip.Reader
			readerIntf = gzipReaderPool.Get()
			buffer     *bytes.Buffer
			bufferIntf = bufferPool.Get()
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

		expectedSize := len(data) * decompressionRatio
		if bufferIntf == nil || buffer.Cap()-buffer.Len() < expectedSize {
			buffer = bytes.NewBuffer(make([]byte, 0, expectedSize+bufferSize))
		} else {
			buffer = bufferIntf.(*bytes.Buffer)
		}
		defer bufferPool.Put(buffer)

		if err := reader.Reset(bytes.NewReader(data)); err != nil {
			return nil, err
		}
		_, err = io.Copy(buffer, reader)
		buffer.Bytes()
		return buffer.Bytes(), err
		//return ioutil.ReadAll(reader)
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
