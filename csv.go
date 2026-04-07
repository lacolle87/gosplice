package gosplice

import (
	"encoding/csv"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"sync"
)

// ---------------------------------------------------------------------------
// CSVConfig
// ---------------------------------------------------------------------------

// CSVConfig controls CSV reading and writing behavior.
type CSVConfig struct {
	// Comma is the field delimiter. Defaults to ',' if zero.
	Comma rune

	// Comment is the comment character. Lines beginning with this rune
	// are skipped by the reader. Zero means no comment support.
	Comment rune

	// Header controls header row behavior.
	// For reading: if true, the first row is treated as column names
	// and used to map struct fields via `csv:"name"` tags.
	// If false, fields are mapped by position index.
	// For writing: if true, a header row is written before data rows.
	Header bool

	// LazyQuotes allows non-standard quoting (passed to encoding/csv).
	LazyQuotes bool

	// TrimLeadingSpace trims leading whitespace from fields.
	TrimLeadingSpace bool
}

func (c CSVConfig) comma() rune {
	if c.Comma == 0 {
		return ','
	}
	return c.Comma
}

// ---------------------------------------------------------------------------
// Internal types used by generic functions (cannot be declared inside them)
// ---------------------------------------------------------------------------

type fieldMapping struct {
	index int
	field int
	kind  reflect.Kind
}

type writeField struct {
	index int
	name  string
}

// ---------------------------------------------------------------------------
// Source — functional (zero-reflect)
// ---------------------------------------------------------------------------

// FromCSVFunc creates a streaming Pipeline[T] from CSV data.
// The mapper converts each raw row into T. Errors skip the row;
// first error is available via pipeline.Err().
func FromCSVFunc[T any](r io.Reader, cfg CSVConfig, mapper func(row []string) (T, error)) *Pipeline[T] {
	cr := csv.NewReader(r)
	cr.Comma = cfg.comma()
	cr.Comment = cfg.Comment
	cr.LazyQuotes = cfg.LazyQuotes
	cr.TrimLeadingSpace = cfg.TrimLeadingSpace
	cr.ReuseRecord = true

	src := &csvFuncSource[T]{
		reader: cr,
		mapper: mapper,
		skip:   cfg.Header,
	}
	return newPipeline[T](src)
}

type csvFuncSource[T any] struct {
	reader *csv.Reader
	mapper func([]string) (T, error)
	skip   bool
	once   sync.Once
	err    error
}

func (s *csvFuncSource[T]) Next() (T, bool) {
	if s.skip {
		s.skip = false
		if _, err := s.reader.Read(); err != nil {
			s.once.Do(func() {
				if err != io.EOF {
					s.err = err
				}
			})
			var zero T
			return zero, false
		}
	}

	for {
		record, err := s.reader.Read()
		if err != nil {
			s.once.Do(func() {
				if err != io.EOF {
					s.err = err
				}
			})
			var zero T
			return zero, false
		}

		v, mapErr := s.mapper(record)
		if mapErr != nil {
			s.once.Do(func() { s.err = mapErr })
			continue
		}
		return v, true
	}
}

func (s *csvFuncSource[T]) Err() error { return s.err }

// ---------------------------------------------------------------------------
// Source — struct tags (reflect-once)
// ---------------------------------------------------------------------------

// FromCSV creates a streaming Pipeline[T] from CSV data using struct tags.
// T must be a struct. Fields map via `csv:"name"` tags (with header) or by position.
// Reflect runs once at init; per-row decoding uses cached field indices.
func FromCSV[T any](r io.Reader, cfg CSVConfig) *Pipeline[T] {
	cr := csv.NewReader(r)
	cr.Comma = cfg.comma()
	cr.Comment = cfg.Comment
	cr.LazyQuotes = cfg.LazyQuotes
	cr.TrimLeadingSpace = cfg.TrimLeadingSpace
	cr.ReuseRecord = true

	src := &csvStructSource[T]{
		reader:    cr,
		hasHeader: cfg.Header,
	}
	return newPipeline[T](src)
}

type csvStructSource[T any] struct {
	reader    *csv.Reader
	hasHeader bool
	mappings  []fieldMapping
	inited    bool
	once      sync.Once
	err       error
}

func (s *csvStructSource[T]) init() {
	var zero T
	t := reflect.TypeOf(zero)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		s.err = fmt.Errorf("gosplice: FromCSV requires a struct type, got %s", t.Kind())
		return
	}

	if s.hasHeader {
		header, err := s.reader.Read()
		if err != nil {
			if err != io.EOF {
				s.err = err
			}
			return
		}
		colIdx := make(map[string]int, len(header))
		for i, name := range header {
			colIdx[name] = i
		}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() {
				continue
			}
			tag := f.Tag.Get("csv")
			if tag == "" || tag == "-" {
				continue
			}
			if idx, ok := colIdx[tag]; ok {
				s.mappings = append(s.mappings, fieldMapping{
					index: idx,
					field: i,
					kind:  f.Type.Kind(),
				})
			}
		}
	} else {
		col := 0
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() {
				continue
			}
			tag := f.Tag.Get("csv")
			if tag == "-" {
				continue
			}
			s.mappings = append(s.mappings, fieldMapping{
				index: col,
				field: i,
				kind:  f.Type.Kind(),
			})
			col++
		}
	}
	s.inited = true
}

func (s *csvStructSource[T]) Next() (T, bool) {
	if !s.inited {
		s.init()
		if s.err != nil {
			var zero T
			return zero, false
		}
	}

	var zero T
	for {
		record, err := s.reader.Read()
		if err != nil {
			s.once.Do(func() {
				if err != io.EOF {
					s.err = err
				}
			})
			return zero, false
		}

		v := reflect.New(reflect.TypeOf(zero)).Elem()
		parseErr := false
		for _, m := range s.mappings {
			if m.index >= len(record) {
				continue
			}
			raw := record[m.index]
			fv := v.Field(m.field)

			if err := setField(fv, raw, m.kind); err != nil {
				s.once.Do(func() {
					s.err = fmt.Errorf("field %d col %d: %w", m.field, m.index, err)
				})
				parseErr = true
				break
			}
		}
		if parseErr {
			continue
		}

		result := v.Interface().(T)
		return result, true
	}
}

func (s *csvStructSource[T]) Err() error { return s.err }

func setField(fv reflect.Value, raw string, kind reflect.Kind) error {
	switch kind {
	case reflect.String:
		fv.SetString(raw)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return fmt.Errorf("parse int %q: %w", raw, err)
		}
		fv.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return fmt.Errorf("parse uint %q: %w", raw, err)
		}
		fv.SetUint(n)
	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return fmt.Errorf("parse float %q: %w", raw, err)
		}
		fv.SetFloat(f)
	case reflect.Bool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return fmt.Errorf("parse bool %q: %w", raw, err)
		}
		fv.SetBool(b)
	default:
		return fmt.Errorf("unsupported type %s", kind)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Sink — functional (zero-reflect)
// ---------------------------------------------------------------------------

// ToCSV writes all pipeline elements as CSV rows to w.
// The format function converts each element into a []string row.
// If cfg.Header is true, headerRow is written first.
func ToCSV[T any](p *Pipeline[T], w io.Writer, cfg CSVConfig, headerRow []string, format func(T) []string) error {
	cw := csv.NewWriter(w)
	cw.Comma = cfg.comma()
	defer cw.Flush()

	if cfg.Header && len(headerRow) > 0 {
		if err := cw.Write(headerRow); err != nil {
			return err
		}
	}

	var writeErr error
	p.ForEach(func(v T) {
		if writeErr != nil {
			return
		}
		writeErr = cw.Write(format(v))
	})

	if writeErr != nil {
		return writeErr
	}
	cw.Flush()
	return cw.Error()
}

// ---------------------------------------------------------------------------
// Sink — struct tags (reflect-once)
// ---------------------------------------------------------------------------

// ToCSVStruct writes all pipeline elements as CSV rows using `csv:"name"` struct tags.
// Reflect runs once to build the field list; per-row serialization uses cached indices.
func ToCSVStruct[T any](p *Pipeline[T], w io.Writer, cfg CSVConfig) error {
	var zero T
	t := reflect.TypeOf(zero)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return fmt.Errorf("gosplice: ToCSVStruct requires a struct type, got %s", t.Kind())
	}

	var fields []writeField
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		tag := f.Tag.Get("csv")
		if tag == "" || tag == "-" {
			continue
		}
		fields = append(fields, writeField{index: i, name: tag})
	}

	cw := csv.NewWriter(w)
	cw.Comma = cfg.comma()
	defer cw.Flush()

	if cfg.Header {
		header := make([]string, len(fields))
		for i, f := range fields {
			header[i] = f.name
		}
		if err := cw.Write(header); err != nil {
			return err
		}
	}

	row := make([]string, len(fields))
	var writeErr error
	p.ForEach(func(v T) {
		if writeErr != nil {
			return
		}
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		}
		for i, f := range fields {
			row[i] = fmt.Sprint(rv.Field(f.index).Interface())
		}
		writeErr = cw.Write(row)
	})

	if writeErr != nil {
		return writeErr
	}
	cw.Flush()
	return cw.Error()
}
