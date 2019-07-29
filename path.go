package s3

func hasTrailingSlash(s string) bool {
	return len(s) > 0 && s[len(s)-1] == '/'
}

func trimLeadingSlash(s string) string {
	if len(s) > 0 && s[0] == '/' {
		return s[1:]
	}
	return s
}

func trimTrailingSlash(s string) string {
	last := len(s) - 1
	for len(s) > 0 && s[last] == '/' {
		s = s[:last]
		last--
	}
	return s
}

func addTrailingSlash(s string) string {
	last := len(s) - 1
	if len(s) > 0 && s[last] != '/' {
		return s + PathSeparator
	}
	return s
}

func depth(s string) int {
	d := 0
	for _, c := range s {
		if c == '/' {
			d++
		}
	}
	return d
}
