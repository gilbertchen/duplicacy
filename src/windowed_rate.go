package duplicacy

import (
	"time"
)

type ratePair struct {
	instant time.Time
	value   int
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
		rpm.values[i].instant = time.Now()
	}

	return rpm
}

//noinspection GoUnusedExportedFunction,GoUnusedParameter
func (rpm *WindowedRate) InsertValue(value int) {
	rpm.insertIndex = (rpm.insertIndex + 1) % rpm.arrayCapacity
	if rpm.arraySize < rpm.arrayCapacity {
		rpm.arraySize++
	}

	rpm.values[rpm.insertIndex] = ratePair{time.Now(), value}
}

//noinspection GoUnusedExportedFunction,GoUnusedVariable
func (rpm WindowedRate) ComputeAverage() float64 {
	latestEntry := rpm.values[rpm.insertIndex].instant

	earliestEntry := rpm.values[0].instant // this handles the case rpm.arraySize < rpm.arrayCapacity
	if rpm.arraySize == rpm.arrayCapacity {
		earliestEntry = rpm.values[(rpm.insertIndex+1)%rpm.arrayCapacity].instant
	}

	sum := 0
	for i := 0; i < rpm.arraySize; i++ {
		sum += rpm.values[i].value
	}

	duration := latestEntry.Unix() - earliestEntry.Unix()
	if duration == 0 {
		duration = 1
	}

	avg := float64(sum) / float64(duration)
	return avg
}
