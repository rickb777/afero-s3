// A simple type derived from []FileInfo
// Not thread-safe.
//
// Generated from simple/list.tpl with Type=FileInfo
// options: Comparable:true Numeric:<no value> Ordered:<no value> Stringer:<no value>
// GobEncode:<no value> Mutable:always ToList:always ToSet:<no value>
// by runtemplate v3.4.2
// See https://github.com/rickb777/runtemplate/blob/master/v3/BUILTIN.md

package s3

import (
	"math/rand"
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

// NewFileInfoList constructs a new list containing the supplied values, if any.
func NewFileInfoList(values ...FileInfo) FileInfoList {
	list := MakeFileInfoList(len(values), len(values))
	copy(list, values)
	return list
}

// ConvertFileInfoList constructs a new list containing the supplied values, if any.
// The returned boolean will be false if any of the values could not be converted correctly.
// The returned list will contain all the values that were correctly converted.
func ConvertFileInfoList(values ...interface{}) (FileInfoList, bool) {
	list := MakeFileInfoList(0, len(values))

	for _, i := range values {
		switch j := i.(type) {
		case FileInfo:
			list = append(list, j)
		case *FileInfo:
			list = append(list, *j)
		}
	}

	return list, len(list) == len(values)
}

// BuildFileInfoListFromChan constructs a new FileInfoList from a channel that supplies a sequence
// of values until it is closed. The function doesn't return until then.
func BuildFileInfoListFromChan(source <-chan FileInfo) FileInfoList {
	list := MakeFileInfoList(0, 0)
	for v := range source {
		list = append(list, v)
	}
	return list
}

//-------------------------------------------------------------------------------------------------

// IsSequence returns true for lists and queues.
func (list FileInfoList) IsSequence() bool {
	return true
}

// IsSet returns false for lists or queues.
func (list FileInfoList) IsSet() bool {
	return false
}

// slice returns the internal elements of the current list. This is a seam for testing etc.
func (list FileInfoList) slice() []FileInfo {
	return list
}

// ToList returns the elements of the list as a list, which is an identity operation in this case.
func (list FileInfoList) ToList() FileInfoList {
	return list
}

// ToSlice returns the elements of the list as a slice, which is an identity operation in this case,
// because the simple list is merely a dressed-up slice.
func (list FileInfoList) ToSlice() []FileInfo {
	return list
}

// ToInterfaceSlice returns the elements of the current list as a slice of arbitrary type.
func (list FileInfoList) ToInterfaceSlice() []interface{} {
	s := make([]interface{}, 0, len(list))
	for _, v := range list {
		s = append(s, v)
	}
	return s
}

// Clone returns a shallow copy of the list. It does not clone the underlying elements.
func (list FileInfoList) Clone() FileInfoList {
	return NewFileInfoList(list...)
}

//-------------------------------------------------------------------------------------------------

// Get gets the specified element in the list.
// Panics if the index is out of range or the list is nil.
// The simple list is a dressed-up slice and normal slice operations will also work.
func (list FileInfoList) Get(i int) FileInfo {
	return list[i]
}

// Head gets the first element in the list. Head plus Tail include the whole list. Head is the opposite of Last.
// Panics if list is empty or nil.
func (list FileInfoList) Head() FileInfo {
	return list[0]
}

// HeadOption gets the first element in the list, if possible.
// Otherwise returns the zero value.
func (list FileInfoList) HeadOption() FileInfo {
	if list.IsEmpty() {
		return *(new(FileInfo))
	}
	return list[0]
}

// Last gets the last element in the list. Init plus Last include the whole list. Last is the opposite of Head.
// Panics if list is empty or nil.
func (list FileInfoList) Last() FileInfo {
	return list[len(list)-1]
}

// LastOption gets the last element in the list, if possible.
// Otherwise returns the zero value.
func (list FileInfoList) LastOption() FileInfo {
	if list.IsEmpty() {
		return *(new(FileInfo))
	}
	return list[len(list)-1]
}

// Tail gets everything except the head. Head plus Tail include the whole list. Tail is the opposite of Init.
// Panics if list is empty or nil.
func (list FileInfoList) Tail() FileInfoList {
	return list[1:]
}

// Init gets everything except the last. Init plus Last include the whole list. Init is the opposite of Tail.
// Panics if list is empty or nil.
func (list FileInfoList) Init() FileInfoList {
	return list[:len(list)-1]
}

// IsEmpty tests whether FileInfoList is empty.
func (list FileInfoList) IsEmpty() bool {
	return list.Size() == 0
}

// NonEmpty tests whether FileInfoList is empty.
func (list FileInfoList) NonEmpty() bool {
	return list.Size() > 0
}

//-------------------------------------------------------------------------------------------------

// Size returns the number of items in the list - an alias of Len().
func (list FileInfoList) Size() int {
	return len(list)
}

// Len returns the number of items in the list - an alias of Size().
// This is one of the three methods in the standard sort.Interface.
func (list FileInfoList) Len() int {
	return len(list)
}

// Swap exchanges two elements, which is necessary during sorting etc.
// This is one of the three methods in the standard sort.Interface.
func (list FileInfoList) Swap(i, j int) {
	list[i], list[j] = list[j], list[i]
}

//-------------------------------------------------------------------------------------------------

// Contains determines whether a given item is already in the list, returning true if so.
func (list FileInfoList) Contains(v FileInfo) bool {
	return list.Exists(func(x FileInfo) bool {
		return x == v
	})
}

// ContainsAll determines whether the given items are all in the list, returning true if so.
// This is potentially a slow method and should only be used rarely.
func (list FileInfoList) ContainsAll(i ...FileInfo) bool {
	for _, v := range i {
		if !list.Contains(v) {
			return false
		}
	}
	return true
}

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

// Send returns a channel that will send all the elements in order.
// A goroutine is created to send the elements; this only terminates when all the elements
// have been consumed. The channel will be closed when all the elements have been sent.
func (list FileInfoList) Send() <-chan FileInfo {
	ch := make(chan FileInfo)
	go func() {
		for _, v := range list {
			ch <- v
		}
		close(ch)
	}()
	return ch
}

//-------------------------------------------------------------------------------------------------

// Reverse returns a copy of FileInfoList with all elements in the reverse order.
//
// The original list is not modified.
func (list FileInfoList) Reverse() FileInfoList {
	n := len(list)
	result := MakeFileInfoList(n, n)
	last := n - 1
	for i, v := range list {
		result[last-i] = v
	}
	return result
}

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

//-------------------------------------------------------------------------------------------------

// Shuffle returns a shuffled copy of FileInfoList, using a version of the Fisher-Yates shuffle.
//
// The original list is not modified.
func (list FileInfoList) Shuffle() FileInfoList {
	if list == nil {
		return nil
	}

	return list.Clone().DoShuffle()
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

// Take returns a slice of FileInfoList containing the leading n elements of the source list.
// If n is greater than or equal to the size of the list, the whole original list is returned.
func (list FileInfoList) Take(n int) FileInfoList {
	if n >= len(list) {
		return list
	}
	return list[0:n]
}

// Drop returns a slice of FileInfoList without the leading n elements of the source list.
// If n is greater than or equal to the size of the list, an empty list is returned.
//
// The original list is not modified.
func (list FileInfoList) Drop(n int) FileInfoList {
	if n == 0 {
		return list
	}

	l := len(list)
	if n < l {
		return list[n:]
	}
	return list[l:]
}

// TakeLast returns a slice of FileInfoList containing the trailing n elements of the source list.
// If n is greater than or equal to the size of the list, the whole original list is returned.
//
// The original list is not modified.
func (list FileInfoList) TakeLast(n int) FileInfoList {
	l := len(list)
	if n >= l {
		return list
	}
	return list[l-n:]
}

// DropLast returns a slice of FileInfoList without the trailing n elements of the source list.
// If n is greater than or equal to the size of the list, an empty list is returned.
//
// The original list is not modified.
func (list FileInfoList) DropLast(n int) FileInfoList {
	if n == 0 {
		return list
	}

	l := len(list)
	if n > l {
		return list[l:]
	}
	return list[0 : l-n]
}

// TakeWhile returns a new FileInfoList containing the leading elements of the source list. Whilst the
// predicate p returns true, elements are added to the result. Once predicate p returns false, all remaining
// elements are excluded.
//
// The original list is not modified.
func (list FileInfoList) TakeWhile(p func(FileInfo) bool) FileInfoList {
	result := MakeFileInfoList(0, 0)
	for _, v := range list {
		if p(v) {
			result = append(result, v)
		} else {
			return result
		}
	}
	return result
}

// DropWhile returns a new FileInfoList containing the trailing elements of the source list. Whilst the
// predicate p returns true, elements are excluded from the result. Once predicate p returns false, all remaining
// elements are added.
//
// The original list is not modified.
func (list FileInfoList) DropWhile(p func(FileInfo) bool) FileInfoList {
	result := MakeFileInfoList(0, 0)
	adding := false

	for _, v := range list {
		if adding || !p(v) {
			adding = true
			result = append(result, v)
		}
	}

	return result
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

// Map returns a new FileInfoList by transforming every element with function f.
// The resulting list is the same size as the original list.
// The original list is not modified.
//
// This is a domain-to-range mapping function. For bespoke transformations to other types, copy and modify
// this method appropriately.
func (list FileInfoList) Map(f func(FileInfo) FileInfo) FileInfoList {
	result := MakeFileInfoList(0, len(list))

	for _, v := range list {
		result = append(result, f(v))
	}

	return result
}

// FlatMap returns a new FileInfoList by transforming every element with function f that
// returns zero or more items in a slice. The resulting list may have a different size to the original list.
// The original list is not modified.
//
// This is a domain-to-range mapping function. For bespoke transformations to other types, copy and modify
// this method appropriately.
func (list FileInfoList) FlatMap(f func(FileInfo) []FileInfo) FileInfoList {
	result := MakeFileInfoList(0, len(list))

	for _, v := range list {
		result = append(result, f(v)...)
	}

	return result
}

// CountBy gives the number elements of FileInfoList that return true for the predicate p.
func (list FileInfoList) CountBy(p func(FileInfo) bool) (result int) {
	for _, v := range list {
		if p(v) {
			result++
		}
	}
	return
}

// MinBy returns an element of FileInfoList containing the minimum value, when compared to other elements
// using a passed func defining ‘less’. In the case of multiple items being equally minimal, the first such
// element is returned. Panics if there are no elements.
func (list FileInfoList) MinBy(less func(FileInfo, FileInfo) bool) FileInfo {
	l := len(list)
	if l == 0 {
		panic("Cannot determine the minimum of an empty list.")
	}

	m := 0
	for i := 1; i < l; i++ {
		if less(list[i], list[m]) {
			m = i
		}
	}

	return list[m]
}

// MaxBy returns an element of FileInfoList containing the maximum value, when compared to other elements
// using a passed func defining ‘less’. In the case of multiple items being equally maximal, the first such
// element is returned. Panics if there are no elements.
func (list FileInfoList) MaxBy(less func(FileInfo, FileInfo) bool) FileInfo {
	l := len(list)
	if l == 0 {
		panic("Cannot determine the maximum of an empty list.")
	}

	m := 0
	for i := 1; i < l; i++ {
		if less(list[m], list[i]) {
			m = i
		}
	}

	return list[m]
}

// DistinctBy returns a new FileInfoList whose elements are unique, where equality is defined by the equal function.
func (list FileInfoList) DistinctBy(equal func(FileInfo, FileInfo) bool) FileInfoList {
	result := MakeFileInfoList(0, len(list))
Outer:
	for _, v := range list {
		for _, r := range result {
			if equal(v, r) {
				continue Outer
			}
		}
		result = append(result, v)
	}
	return result
}

// IndexWhere finds the index of the first element satisfying predicate p. If none exists, -1 is returned.
func (list FileInfoList) IndexWhere(p func(FileInfo) bool) int {
	return list.IndexWhere2(p, 0)
}

// IndexWhere2 finds the index of the first element satisfying predicate p at or after some start index.
// If none exists, -1 is returned.
func (list FileInfoList) IndexWhere2(p func(FileInfo) bool, from int) int {
	for i, v := range list {
		if i >= from && p(v) {
			return i
		}
	}
	return -1
}

// LastIndexWhere finds the index of the last element satisfying predicate p.
// If none exists, -1 is returned.
func (list FileInfoList) LastIndexWhere(p func(FileInfo) bool) int {
	return list.LastIndexWhere2(p, len(list))
}

// LastIndexWhere2 finds the index of the last element satisfying predicate p at or before some start index.
// If none exists, -1 is returned.
func (list FileInfoList) LastIndexWhere2(p func(FileInfo) bool, before int) int {
	if before < 0 {
		before = len(list)
	}
	for i := len(list) - 1; i >= 0; i-- {
		v := list[i]
		if i <= before && p(v) {
			return i
		}
	}
	return -1
}

//-------------------------------------------------------------------------------------------------
// These methods are included when FileInfo is comparable.

// Equals determines if two lists are equal to each other.
// If they both are the same size and have the same items in the same order, they are considered equal.
// Order of items is not relevent for sets to be equal.
func (list FileInfoList) Equals(other FileInfoList) bool {
	if list == nil {
		return len(other) == 0
	}

	if other == nil {
		return len(list) == 0
	}

	if len(list) != len(other) {
		return false
	}

	for i, v := range list {
		if v != other[i] {
			return false
		}
	}

	return true
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
