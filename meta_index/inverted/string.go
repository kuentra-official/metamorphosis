package inverted

import (
	"context"
	"kuentra-official/metamorphosis/meta_disk/v1disk"
	"kuentra-official/metamorphosis/meta_package/models"
	"kuentra-official/metamorphosis/meta_package/submodules"
	"strings"

	"github.com/RoaringBitmap/roaring/roaring64"
)

type IndexInvertedString struct {
	inner  *IndexInverted[string]
	params models.IndexStringParameters
}

func NewIndexInvertedString(bucket v1disk.Bucket, params models.IndexStringParameters) *IndexInvertedString {
	inv := NewIndexInverted[string](bucket)
	return &IndexInvertedString{inner: inv, params: params}
}

func (inv *IndexInvertedString) InsertUpdateDelete(ctx context.Context, in <-chan IndexChange[string]) <-chan error {
	// Process any transformers such as lowercasing before inserting
	out := in
	// Do we need to pre process?
	if !inv.params.CaseSensitive {
		// We ignore the error bev1disk transform function below never
		// returns one
		out, _ = submodules.TransformWithContext(ctx, in, func(change IndexChange[string]) (IndexChange[string], bool, error) {
			if change.CurrentData != nil {
				*change.CurrentData = strings.ToLower(*change.CurrentData)
			}
			if change.PreviousData != nil {
				*change.PreviousData = strings.ToLower(*change.PreviousData)
			}
			return change, false, nil
		})
	}
	return inv.inner.InsertUpdateDelete(ctx, out)
}

func (inv *IndexInvertedString) Search(options models.SearchStringOptions) (*roaring64.Bitmap, error) {
	query := options.Value
	if !inv.params.CaseSensitive {
		query = strings.ToLower(query)
	}
	return inv.inner.Search(query, options.EndValue, options.Operator)
}

// ---------------------------

type IndexInvertedArrayString struct {
	inner  *IndexInvertedArray[string]
	params models.IndexStringArrayParameters
}

func NewIndexInvertedArrayString(bucket v1disk.Bucket, params models.IndexStringArrayParameters) *IndexInvertedArrayString {
	inv := NewIndexInvertedArray[string](bucket)
	return &IndexInvertedArrayString{inner: inv, params: params}
}

func (inv *IndexInvertedArrayString) InsertUpdateDelete(ctx context.Context, in <-chan IndexArrayChange[string]) <-chan error {
	// Process any transformers such as lowercasing before inserting
	out := in
	// Do we need to pre process?
	if !inv.params.CaseSensitive {
		out, _ = submodules.TransformWithContext(ctx, in, func(change IndexArrayChange[string]) (IndexArrayChange[string], bool, error) {
			for i := range change.CurrentData {
				change.CurrentData[i] = strings.ToLower(change.CurrentData[i])
			}
			for i := range change.PreviousData {
				change.PreviousData[i] = strings.ToLower(change.PreviousData[i])
			}
			return change, false, nil
		})
	}
	return inv.inner.InsertUpdateDelete(ctx, out)
}

func (inv *IndexInvertedArrayString) Search(options models.SearchStringArrayOptions) (*roaring64.Bitmap, error) {
	query := options.Value
	if !inv.params.CaseSensitive {
		for i := range query {
			query[i] = strings.ToLower(query[i])
		}
	}
	return inv.inner.Search(query, options.Operator)
}
