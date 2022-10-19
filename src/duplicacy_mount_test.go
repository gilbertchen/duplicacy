package duplicacy

import "testing"

type AssertVal struct {
	result mountReadParams
	err    error
}

func assert(t *testing.T, result AssertVal, expected AssertVal) {
	if result != expected {
		t.Errorf("failed: %v != %v", result, expected)
	}
}

func TestCalculateChunkReadParams(t *testing.T) {
	{
		_, err := calculateChunkReadParams([]int{}, &mountChunkInfo{}, 0)
		if err == nil {
			t.Errorf("should error on empty chunkLengths")
		}
	}

	{
		_, err := calculateChunkReadParams(
			[]int{0},
			&mountChunkInfo{
				EndChunk: 3,
			},
			0)
		if err == nil {
			t.Errorf("should error on chunkLengths being too small")
		}
	}

	{
		_, err := calculateChunkReadParams(
			[]int{0},
			&mountChunkInfo{},
			-1)
		if err == nil {
			t.Errorf("should error on negative ofst")
		}
	}

	{
		_, err := calculateChunkReadParams(
			[]int{10, 20},
			&mountChunkInfo{
				EndChunk:  1,
				EndOffset: 25,
			},
			11)
		if err == nil {
			t.Errorf("should error on EndOffset greater than chunk length")
		}
	}

	{
		res, err := calculateChunkReadParams(
			[]int{45, 37, 98},
			&mountChunkInfo{
				StartChunk:  0,
				StartOffset: 5,
				EndChunk:    0,
				EndOffset:   15,
			},
			0)

		assert(t, AssertVal{res, err}, AssertVal{mountReadParams{0, 5, 15}, nil})
	}

	{
		res, err := calculateChunkReadParams(
			[]int{45, 37, 98},
			&mountChunkInfo{
				StartChunk:  0,
				StartOffset: 5,
				EndChunk:    0,
				EndOffset:   15,
			},
			4)

		assert(t, AssertVal{res, err}, AssertVal{mountReadParams{0, 9, 15}, nil})
	}

	{
		res, err := calculateChunkReadParams(
			[]int{45, 37, 98},
			&mountChunkInfo{
				StartChunk:  0,
				StartOffset: 5,
				EndChunk:    1,
				EndOffset:   15,
			},
			0)

		assert(t, AssertVal{res, err}, AssertVal{mountReadParams{0, 5, 45}, nil})
	}

	{
		res, err := calculateChunkReadParams(
			[]int{45, 37, 98},
			&mountChunkInfo{
				StartChunk:  0,
				StartOffset: 5,
				EndChunk:    1,
				EndOffset:   15,
			},
			4)

		assert(t, AssertVal{res, err}, AssertVal{mountReadParams{0, 9, 45}, nil})
	}

	{
		res, err := calculateChunkReadParams(
			[]int{45, 37, 98},
			&mountChunkInfo{
				StartChunk:  0,
				StartOffset: 5,
				EndChunk:    1,
				EndOffset:   15,
			},
			45)

		assert(t, AssertVal{res, err}, AssertVal{mountReadParams{1, 5, 15}, nil})
	}

	{
		res, err := calculateChunkReadParams(
			[]int{45, 37, 98},
			&mountChunkInfo{
				StartChunk:  0,
				StartOffset: 5,
				EndChunk:    1,
				EndOffset:   15,
			},
			40)

		assert(t, AssertVal{res, err}, AssertVal{mountReadParams{1, 0, 15}, nil})
	}

	{
		res, err := calculateChunkReadParams(
			[]int{45, 37, 98},
			&mountChunkInfo{
				StartChunk:  0,
				StartOffset: 5,
				EndChunk:    1,
				EndOffset:   15,
			},
			60)

		assert(t, AssertVal{res, err}, AssertVal{mountReadParams{1, 15, 15}, nil})
	}

	{
		res, err := calculateChunkReadParams(
			[]int{45, 37, 98},
			&mountChunkInfo{
				StartChunk:  1,
				StartOffset: 7,
				EndChunk:    2,
				EndOffset:   96,
			},
			0)

		assert(t, AssertVal{res, err}, AssertVal{mountReadParams{1, 7, 37}, nil})
	}

	{
		res, err := calculateChunkReadParams(
			[]int{45, 37, 98},
			&mountChunkInfo{
				StartChunk:  0,
				StartOffset: 0,
				EndChunk:    2,
				EndOffset:   98,
			},
			99)

		assert(t, AssertVal{res, err}, AssertVal{mountReadParams{2, 17, 98}, nil})
	}
}
