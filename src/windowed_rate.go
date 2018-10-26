package duplicacy

import (
	"time"
)

type ratePair struct {
	insertedTime time.Time
	value        int64
}

/**
The principle of this WindowedRate struct and algorithm is that it
stores the time instant when a specific value ("uploaded quantity") was inserted,
computes an average of all those values and then divides that average by how much time
it took to upload them.

A circular array of fixed length is used so that new records will simply replace the old ones
as more data is uploaded.
*/
type WindowedRate struct {
	arrayCapacity int
	insertIndex   int
	arraySize     int

	values []ratePair
}

func NewWindowedRate(arrayCapacity int) WindowedRate {
	rpm := WindowedRate{}
	rpm.arrayCapacity = arrayCapacity
	rpm.insertIndex = -1
	rpm.values = make([]ratePair, arrayCapacity)

	for i := 0; i < arrayCapacity; i++ {
		rpm.values[i].insertedTime = time.Now()
	}

	return rpm
}

/**
InsertValue inserts a value in the circular array along with the current time
*/
func (rpm *WindowedRate) InsertValue(value int64) {
	rpm.insertIndex = (rpm.insertIndex + 1) % rpm.arrayCapacity
	if rpm.arraySize < rpm.arrayCapacity {
		rpm.arraySize++
	}

	rpm.values[rpm.insertIndex] = ratePair{time.Now(), value}
}

/**
ComputeAverage calculates the average transfer speed between
the first entry in the array (firstEntry) and
the last entry in the array (latestEntry).

It handles the case where the array was not filled completely
(we are early in the upload)
*/
func (rpm WindowedRate) ComputeAverage() int64 {
	latestEntry := rpm.values[rpm.insertIndex]

	firstEntry := rpm.values[0] // this handles the case rpm.arraySize < rpm.arrayCapacity
	if rpm.arraySize == rpm.arrayCapacity {
		firstEntry = rpm.values[(rpm.insertIndex+1)%rpm.arrayCapacity]
	}

	totalTransferred := latestEntry.value - firstEntry.value

	duration := latestEntry.insertedTime.Unix() - firstEntry.insertedTime.Unix()
	if duration == 0 {
		duration = 1
	}

	avg := totalTransferred / duration
	return avg
}
