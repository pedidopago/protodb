package protodb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCensorWord(t *testing.T) {
	assert.Equal(t, "b*b", CensorWord("bob"))
	assert.Equal(t, "**", CensorWord("hi"))
	assert.Equal(t, "b*******r", CensorWord("baconator"))
	assert.Equal(t, "ex************************g", CensorWord("extremelyveryuberlongstring"))
}
