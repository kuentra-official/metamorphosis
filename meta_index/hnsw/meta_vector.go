package hnsw

import "io"

var _ Embeddable = MetaVector{}

// Vector is a struct that holds an ID and an embedding
// and implements the Embeddable interface.
type MetaVector struct {
	id        string
	embedding []float32
}

// MakeVector creates a new Vector with the given ID and embedding.
func MakeVector(id string, embedding []float32) MetaVector {
	return MetaVector{
		id:        id,
		embedding: embedding,
	}
}

func (v MetaVector) ID() string {
	return v.id
}

func (v MetaVector) Embedding() []float32 {
	return v.embedding
}

func (v MetaVector) WriteTo(w io.Writer) (int64, error) {
	n, err := multiBinaryWrite(w, v.id, len(v.embedding), v.embedding)
	return int64(n), err
}

func (v *MetaVector) ReadFrom(r io.Reader) (int64, error) {
	var embLen int
	n, err := multiBinaryRead(r, &v.id, &embLen)
	if err != nil {
		return int64(n), err
	}

	v.embedding = make([]float32, embLen)
	n, err = binaryRead(r, &v.embedding)

	return int64(n), err
}

var (
	_ io.WriterTo   = (*MetaVector)(nil)
	_ io.ReaderFrom = (*MetaVector)(nil)
)
