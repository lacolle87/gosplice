package gosplice

import "io"

func (p *Pipeline[T]) ToChannel(ch chan<- T) {
	defer close(ch)
	p.ForEach(func(v T) { ch <- v })
}

func ToWriter[T any](p *Pipeline[T], w io.Writer, encode func(T) []byte) error {
	var writeErr error
	p.ForEach(func(v T) {
		if writeErr != nil {
			return
		}
		_, writeErr = w.Write(encode(v))
	})
	return writeErr
}

func ToWriterString[T any](p *Pipeline[T], w io.Writer, format func(T) string) error {
	var writeErr error
	p.ForEach(func(v T) {
		if writeErr != nil {
			return
		}
		_, writeErr = io.WriteString(w, format(v))
	})
	return writeErr
}
