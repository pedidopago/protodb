package protodb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type privateB struct {
}

type SelStructA struct {
	state         *privateB
	sizeCache     *privateB
	unknownFields *privateB

	// @inject_tag: db:"id"
	StoreId string `protobuf:"bytes,1,opt,name=store_id,json=storeId,proto3" json:"store_id,omitempty" db:"id"`
	// @inject_tag: db:"domain"
	Domain string `protobuf:"bytes,2,opt,name=domain,proto3" json:"domain,omitempty" db:"domain"`
	// @inject_tag: db:"name"
	Name string `protobuf:"bytes,3,opt,name=name,proto3" json:"name,omitempty" db:"name"`
}

func TestSelectColumns(t *testing.T) {
	result := SelectColumns(SelStructA{})
	require.NoError(t, result.Err)
	require.Equal(t, []string{"id", "domain", "name"}, result.Columns)
}
