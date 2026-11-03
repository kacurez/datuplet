package format

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/variant"
)

func TestVariantJSONRendering(t *testing.T) {
	cases := []struct{ in, want string }{
		{`[]`, `[]`},
		{`{"t":[]}`, `{"t":[]}`},
		{`[[]]`, `[[]]`},
		{`{}`, `{}`},
		{`[{},[]]`, `[{},[]]`},
		{`{"a":{"b":[1,[],{"c":[]}]},"z":[[],[[]]]}`, `{"a":{"b":[1,[],{"c":[]}]},"z":[[],[[]]]}`},
		{`"a<b&c>"`, `"a<b&c>"`},
		{`{"k<&>":"v<&>"}`, `{"k<&>":"v<&>"}`},
		{`"quote\" newline\n"`, `"quote\" newline\n"`},
		{`9007199254740993`, `9007199254740993`},
		{`-9223372036854775808`, `-9223372036854775808`},
		{`12345678901234567890.123456789`, `12345678901234567890.123456789`},
		{`1.50`, `1.50`},
		{`3.5`, `3.5`},
		{`true`, `true`},
		{`false`, `false`},
		{`[null,true,false]`, `[null,true,false]`},
		{`{"a":null}`, `{"a":null}`},
	}
	for _, c := range cases {
		v, err := variant.ParseJSON(c.in, false)
		if err != nil {
			t.Fatalf("%s: %v", c.in, err)
		}
		b := extensions.NewVariantBuilder(memory.DefaultAllocator, extensions.NewDefaultVariantType())
		b.Append(v)
		arr := b.NewArray().(*extensions.VariantArray)
		b.Release()
		got, err := VariantJSON(arr, 0)
		arr.Release()
		if err != nil {
			t.Errorf("%s: %v", c.in, err)
			continue
		}
		if string(got) != c.want {
			t.Errorf("render %s = %s, want %s", c.in, got, c.want)
		}
	}
}
