package s3

import (
	"math/rand"
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

// NewFileInfo2List constructs a new list containing the supplied values, if any.
func NewFileInfo2List(values ...FileInfo) FileInfoList {
	list := MakeFileInfoList(len(values), len(values))
	copy(list, values)
	return list
}

// Clone returns a shallow copy of the list. It does not clone the underlying elements.
func (list FileInfoList) Clone() FileInfoList {
	return NewFileInfo2List(list...)
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

// DoReverse alters a FileInfoList with all elements in the reverse order.
// Unlike Reverse, it does not allocate new memory.
//
// The list is modified and the modified list is returned.
func (list FileInfoList) DoReverse() FileInfoList {
	mid := (len(list) + 1) / 2
	last := len(list) - 1
	for i := 0; i < mid; i++ {
		r := last - i
		if i != r {
			list[i], list[r] = list[r], list[i]
		}
	}
	return list
}

// DoShuffle returns a shuffled FileInfoList, using a version of the Fisher-Yates shuffle.
//
// The list is modified and the modified list is returned.
func (list FileInfoList) DoShuffle() FileInfoList {
	if list == nil {
		return nil
	}

	n := len(list)
	for i := 0; i < n; i++ {
		r := i + rand.Intn(n-i)
		list[i], list[r] = list[r], list[i]
	}
	return list
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

// MapTo returns a []string ] by transforming every element with function f.
// The resulting list is the same size as the original list.
// The original list is not modified.
func (list FileInfoList) MapToString(f func(FileInfo) string) []string {
	result := make([]string, 0, len(list))

	for _, v := range list {
		result = append(result, f(v))
	}

	return result
}

//-------------------------------------------------------------------------------------------------

// SortByPath alters the ordering of the list to be by path, 'lowest' string value first.
// This uses a stable sort algorithm.
func (list FileInfoList) SortByPath() FileInfoList {
	return list.StableSortBy(func(i, j FileInfo) bool {
		return i.Path() < j.Path()
	})
}

// SortByDeepestFirst alters the ordering of the list to be by path depth, deepest paths first.
// The path depth is the number of segments separated by '/'.
// This uses a stable sort algorithm.
func (list FileInfoList) SortByDeepestFirst() FileInfoList {
	return list.StableSortBy(func(i, j FileInfo) bool {
		return i.depth > j.depth
	})
}

//-------------------------------------------------------------------------------------------------

// Names gets a list of file names in the same order as this list.
func (list FileInfoList) Names() []string {
	return list.MapToString(func(fi FileInfo) string {
		return fi.Name()
	})
}

// Paths gets a list of file paths in the same order as this list.
func (list FileInfoList) Paths() []string {
	return list.MapToString(func(fi FileInfo) string {
		return fi.Path()
	})
}

// ContainsPath returns true iff the list contains a given file path.
// This uses a linear search that is slow for very large lists.
func (list FileInfoList) ContainsPath(path string) bool {
	return list.Exists(func(fi FileInfo) bool {
		return fi.Path() == path
	})
}

// ContainsName returns true iff the list contains a given file name.
// This uses a linear search that is slow for very large lists.
func (list FileInfoList) ContainsName(name string) bool {
	return list.Exists(func(fi FileInfo) bool {
		return fi.Name() == name
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
