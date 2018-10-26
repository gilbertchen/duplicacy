package duplicacy

import (
	"time"
)

type ratePair struct {
	insertedTime time.Time
	value        int64
}

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

//noinspection GoUnusedExportedFunction,GoUnusedParameter
func (rpm *WindowedRate) InsertValue(value int64) {
	rpm.insertIndex = (rpm.insertIndex + 1) % rpm.arrayCapacity
	if rpm.arraySize < rpm.arrayCapacity {
		rpm.arraySize++
	}

	rpm.values[rpm.insertIndex] = ratePair{time.Now(), value}
}

//noinspection GoUnusedExportedFunction,GoUnusedVariable
func (rpm WindowedRate) ComputeAverage() int64 {
	latestEntry := rpm.values[rpm.insertIndex].insertedTime

	firstEntry := rpm.values[0].insertedTime // this handles the case rpm.arraySize < rpm.arrayCapacity
	if rpm.arraySize == rpm.arrayCapacity {
		firstEntry = rpm.values[(rpm.insertIndex+1)%rpm.arrayCapacity].insertedTime
	}

	sum := int64(0)
	for i := 0; i < rpm.arraySize; i++ {
		sum += rpm.values[i].value
	}
	totalTransferred := sum / int64(rpm.arraySize)

	duration := latestEntry.Unix() - firstEntry.Unix()
	if duration == 0 {
		duration = 1
	}

	avg := totalTransferred / duration
	return avg
}
