package protodb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTransform(t *testing.T) {
	a := struct {
		A string `transform:"email"`
		B string `transform:"notfound"`
	}{}
	a.A = "someone@email.com"
	assert.NoError(t, Transform(&a, map[string]TransformFunc{
		"email": func(i interface{}) interface{} {
			return CensorWord(i.(string))
		},
	}))
	assert.Equal(t, "s***************m", a.A)
	assert.Equal(t, "", a.B)
}
