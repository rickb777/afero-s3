package s3

import "os"

// ToSlice adapts the list to the equivalent slice of the base type.
func (list FileInfoList) ToStdSlice() []os.FileInfo {
	l2 := make([]os.FileInfo, len(list))
	for i, fi := range list {
		l2[i] = fi
	}
	return l2
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
