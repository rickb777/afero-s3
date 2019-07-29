package s3

import (
	"os"
	"sort"
)

// FileInfoList is a slice of type FileInfo. Use it where you would use []FileInfo.
// To add items to the list, simply use the normal built-in append function.
//
// List values follow a similar pattern to Scala Lists and LinearSeqs in particular.
// For comparison with Scala, see e.g. http://www.scala-lang.org/api/2.11.7/#scala.collection.LinearSeq
type FileInfoList []FileInfo

//-------------------------------------------------------------------------------------------------

// MakeFileInfoList makes an empty list with both length and capacity initialised.
func MakeFileInfoList(length, capacity int) FileInfoList {
	return make(FileInfoList, length, capacity)
}

//-------------------------------------------------------------------------------------------------

// ToSlice adapts the list to the equivalent slice of the base type.
func (list FileInfoList) ToSlice() []os.FileInfo {
	l2 := make([]os.FileInfo, len(list))
	for i, fi := range list {
		l2[i] = fi
	}
	return l2
}

//-------------------------------------------------------------------------------------------------

// Exists verifies that one or more elements of FileInfoList return true for the predicate p.
func (list FileInfoList) Exists(p func(FileInfo) bool) bool {
	for _, v := range list {
		if p(v) {
			return true
		}
	}
	return false
}

// Forall verifies that all elements of FileInfoList return true for the predicate p.
func (list FileInfoList) Forall(p func(FileInfo) bool) bool {
	for _, v := range list {
		if !p(v) {
			return false
		}
	}
	return true
}

// Foreach iterates over FileInfoList and executes function f against each element.
func (list FileInfoList) Foreach(f func(FileInfo)) {
	for _, v := range list {
		f(v)
	}
}

//-------------------------------------------------------------------------------------------------

// Find returns the first FileInfo that returns true for predicate p.
// False is returned if none match.
func (list FileInfoList) Find(p func(FileInfo) bool) (FileInfo, bool) {

	for _, v := range list {
		if p(v) {
			return v, true
		}
	}

	var empty FileInfo
	return empty, false
}

// Filter returns a new FileInfoList whose elements return true for predicate p.
//
// The original list is not modified.
func (list FileInfoList) Filter(p func(FileInfo) bool) FileInfoList {
	result := MakeFileInfoList(0, len(list))

	for _, v := range list {
		if p(v) {
			result = append(result, v)
		}
	}

	return result
}

// Partition returns two new FileInfoLists whose elements return true or false for the predicate, p.
// The first result consists of all elements that satisfy the predicate and the second result consists of
// all elements that don't. The relative order of the elements in the results is the same as in the
// original list.
//
// The original list is not modified.
func (list FileInfoList) Partition(p func(FileInfo) bool) (FileInfoList, FileInfoList) {
	matching := MakeFileInfoList(0, len(list))
	others := MakeFileInfoList(0, len(list))

	for _, v := range list {
		if p(v) {
			matching = append(matching, v)
		} else {
			others = append(others, v)
		}
	}

	return matching, others
}

//-------------------------------------------------------------------------------------------------

func (list FileInfoList) SortByPath() FileInfoList {
	return list.StableSortBy(func(i, j FileInfo) bool {
		return i.Path() < j.Path()
	})
}

func (list FileInfoList) SortByDeepestFirst() FileInfoList {
	return list.StableSortBy(func(i, j FileInfo) bool {
		return i.depth > j.depth
	})
}

//-------------------------------------------------------------------------------------------------

type sortableFileInfoList struct {
	less func(i, j FileInfo) bool
	m    []FileInfo
}

func (sl sortableFileInfoList) Less(i, j int) bool {
	return sl.less(sl.m[i], sl.m[j])
}

func (sl sortableFileInfoList) Len() int {
	return len(sl.m)
}

func (sl sortableFileInfoList) Swap(i, j int) {
	sl.m[i], sl.m[j] = sl.m[j], sl.m[i]
}

// SortBy alters the list so that the elements are sorted by a specified ordering.
// Sorting happens in-place; the modified list is returned.
func (list FileInfoList) SortBy(less func(i, j FileInfo) bool) FileInfoList {

	sort.Sort(sortableFileInfoList{less, list})
	return list
}

// StableSortBy alters the list so that the elements are sorted by a specified ordering.
// Sorting happens in-place; the modified list is returned.
// The algorithm keeps the original order of equal elements.
func (list FileInfoList) StableSortBy(less func(i, j FileInfo) bool) FileInfoList {

	sort.Stable(sortableFileInfoList{less, list})
	return list
}
