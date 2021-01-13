package sarama

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"

	snappy "github.com/eapache/go-xerial-snappy"
	"github.com/pierrec/lz4"
)

const (
	readerPoolSize = 16
)

var (
	lz4ReaderPool = NewCloserPool(readerPoolSize, func() interface{} {
		return lz4.NewReader(nil)
	})

	gzipReaderPool = NewCloserPool(readerPoolSize, nil)
)

func decompress(cc CompressionCodec, data []byte) ([]byte, error) {
	switch cc {
	case CompressionNone:
		return data, nil
	case CompressionGZIP:
		var (
			err        error
			reader     *gzip.Reader
			readerIntf = gzipReaderPool.Get()
		)
		if readerIntf != nil {
			reader = readerIntf.(*gzip.Reader)
			if err := reader.Reset(bytes.NewReader(data)); err != nil {
				return nil, err
			}
		} else {
			reader, err = gzip.NewReader(bytes.NewReader(data))
			if err != nil {
				return nil, err
			}
		}

		defer gzipReaderPool.Put(reader)

		return ioutil.ReadAll(reader)
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
