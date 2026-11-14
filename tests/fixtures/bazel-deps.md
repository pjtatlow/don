# Bazel dependency wire fixture

`bazel-deps.pb` is actual Bazel 9.3.2 output, not a protobuf encoded by the
Rust test's partial schema. This catches incorrect field tags and verifies
that unrelated fields can be skipped. It includes aliases, both branches of
an unconfigured select, a generated output, a glob, and an external rule
that points back to a first-party target.

Generated with:

```sh
bazel query 'filter("^//", deps(//:all + //:roundtrip + //generated:generator))' \
  --output=streamed_proto --proto:output_rule_attrs= --noproto:locations
```

The root filegroup `all` refers to `//aliases:chosen`, `//generated:out`, and
an external source. The alias points to `//branches:selected`, which selects
`branches/a.txt` or `branches/b.txt`. The generator takes `generated/*.txt`
and produces `generated/out`. Root `roundtrip` refers to an external rule
that depends on `//common:shared`.

Expected source packages preserve the existing parser's behavior:
`all`, `aliases:chosen`, and `branches:selected` reach `branches`;
`generated:generator` reaches `generated`; `roundtrip` reaches nothing
because external rule nodes are excluded. Generated outputs are not
followed back to their generators.
