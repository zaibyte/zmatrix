package neo

import (
	"fmt"
	"sort"
	"testing"
)

func TestSearch(t *testing.T) {

	s := []int{0, 1, 3}
	fmt.Println(sort.Search(3, func(i int) bool {
		return s[i] == 400
	}))
}
