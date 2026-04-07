package gosplice

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"sync"
)

// Row is a single CSV row with header-aware access, similar to pandas DataFrame rows.
// Access fields by column name or index. Zero-copy — shares memory with the CSV reader
// until you call a conversion method.
type Row struct {
	header []string
	fields []string
	colIdx map[string]int // shared across all rows from the same source
}

// Get returns the field value by column name. Returns "" if not found.
func (r Row) Get(column string) string {
	if idx, ok := r.colIdx[column]; ok && idx < len(r.fields) {
		return r.fields[idx]
	}
	return ""
}

// Index returns the field at position i. Returns "" if out of bounds.
func (r Row) Index(i int) string {
	if i >= 0 && i < len(r.fields) {
		return r.fields[i]
	}
	return ""
}

// GetInt parses the named column as int. Returns 0 and error on failure.
func (r Row) GetInt(column string) (int, error) {
	s := r.Get(column)
	if s == "" {
		return 0, fmt.Errorf("column %q: empty or missing", column)
	}
	return strconv.Atoi(s)
}

// GetFloat parses the named column as float64.
func (r Row) GetFloat(column string) (float64, error) {
	s := r.Get(column)
	if s == "" {
		return 0, fmt.Errorf("column %q: empty or missing", column)
	}
	return strconv.ParseFloat(s, 64)
}

// GetBool parses the named column as bool (accepts "true", "false", "1", "0", etc).
func (r Row) GetBool(column string) (bool, error) {
	s := r.Get(column)
	if s == "" {
		return false, fmt.Errorf("column %q: empty or missing", column)
	}
	return strconv.ParseBool(s)
}

// Has returns true if the column exists in the header.
func (r Row) Has(column string) bool {
	_, ok := r.colIdx[column]
	return ok
}

// Columns returns the header column names. Nil if no header.
func (r Row) Columns() []string { return r.header }

// Len returns the number of fields in this row.
func (r Row) Len() int { return len(r.fields) }

// Fields returns a copy of the raw field values.
func (r Row) Fields() []string {
	out := make([]string, len(r.fields))
	copy(out, r.fields)
	return out
}

// AsMap returns the row as a map[string]string. Only works with headers.
func (r Row) AsMap() map[string]string {
	m := make(map[string]string, len(r.header))
	for i, col := range r.header {
		if i < len(r.fields) {
			m[col] = r.fields[i]
		}
	}
	return m
}

// String returns a debug representation: "Row{name:Alice, age:30, ...}".
func (r Row) String() string {
	if len(r.header) == 0 {
		return fmt.Sprintf("Row%v", r.fields)
	}
	pairs := make([]string, 0, len(r.header))
	for i, col := range r.header {
		v := ""
		if i < len(r.fields) {
			v = r.fields[i]
		}
		pairs = append(pairs, col+":"+v)
	}
	s := "Row{"
	for i, p := range pairs {
		if i > 0 {
			s += ", "
		}
		s += p
	}
	return s + "}"
}

// ---------------------------------------------------------------------------
// FromCSVRows — streaming Pipeline[Row]
// ---------------------------------------------------------------------------

// FromCSVRows creates a streaming Pipeline[Row] from CSV data.
// Each element is a Row with pandas-style access: row.Get("name"), row.GetFloat("price").
// Header row is consumed automatically when cfg.Header is true.
// Without header, use row.Index(0), row.Index(1), etc.
func FromCSVRows(r io.Reader, cfg CSVConfig) *Pipeline[Row] {
	cr := csv.NewReader(r)
	cr.Comma = cfg.comma()
	cr.Comment = cfg.Comment
	cr.LazyQuotes = cfg.LazyQuotes
	cr.TrimLeadingSpace = cfg.TrimLeadingSpace
	cr.FieldsPerRecord = -1 // allow variable-length rows

	src := &csvRowSource{
		reader:    cr,
		hasHeader: cfg.Header,
	}
	return newPipeline[Row](src)
}

type csvRowSource struct {
	reader    *csv.Reader
	hasHeader bool
	header    []string
	colIdx    map[string]int
	inited    bool
	once      sync.Once
	err       error
}

func (s *csvRowSource) Next() (Row, bool) {
	if !s.inited {
		s.inited = true
		if s.hasHeader {
			hdr, err := s.reader.Read()
			if err != nil {
				s.once.Do(func() {
					if err != io.EOF {
						s.err = err
					}
				})
				return Row{}, false
			}
			// Own copy — csv.Reader may reuse the slice
			s.header = make([]string, len(hdr))
			copy(s.header, hdr)
			s.colIdx = make(map[string]int, len(s.header))
			for i, name := range s.header {
				s.colIdx[name] = i
			}
		}
	}

	record, err := s.reader.Read()
	if err != nil {
		s.once.Do(func() {
			if err != io.EOF {
				s.err = err
			}
		})
		return Row{}, false
	}

	// Own copy — ReuseRecord is not set, but be safe
	fields := make([]string, len(record))
	copy(fields, record)

	return Row{
		header: s.header,
		fields: fields,
		colIdx: s.colIdx, // shared, read-only
	}, true
}

func (s *csvRowSource) Err() error { return s.err }
