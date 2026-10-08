"""Property-based checks of the shredding declaration rules (pyhoglake.variant).

The oracle is the documented grammar, not the validator's code: a
generator builds declarations by the rules in the module docstring (the
server's VariantShredding.kt), then breaks at most one of them at a node
it picks. A sound declaration must be accepted and a broken one refused —
the same shape as the server's VariantShreddingFuzzTest, whose request
seeds are also in the shared vector file (test_variant_ddl.py).

Beside them, an exhaustive check: every key of the grammar a node may not
have (another type's, or ``name`` off a field) is refused as unknown at
that node, which the sampled generator reaches too rarely to pin.

Three more properties:

- arbitrary JSON values are refused only with ValidationError, and every
  refusal stays bounded (the server's fuzz target bounds its own at 2048);
- a declaration survives the marker: variant_field, then
  schema_to_column_defs, gives back the declaration it was built from;
- the marker's bytes depend on the declaration only up to object key
  order, which is the equivalence JSONB storage keeps.
"""

import json

import pyarrow as pa
import pytest
from hypothesis import given
from hypothesis import strategies as st

from pyhoglake import ValidationError, variant_field
from pyhoglake.types import schema_to_column_defs
from pyhoglake.variant import (
    DECIMAL_TYPES,
    MAX_PATH_NAME_BYTES,
    MAX_SHREDDING_DEPTH,
    PRIMITIVE_TYPES,
    VARIANT_FIELD_KEY,
    validate_shredding,
)

# The "qe" hypothesis profile (deadline=None) is loaded in conftest.py.

#: The keys of each node type besides "type"; an object field also has
#: "name".
_TYPE_KEYS = {
    "object": {"fields"},
    "array": {"element"},
    **{decimal: {"precision", "scale"} for decimal in DECIMAL_TYPES},
}

#: Keys some node may have, and one none may: an unknown key is drawn from
#: these as well as at random, so a node meets the keys of other types.
_GRAMMAR_KEYS = ["type", "name", "fields", "element", "precision", "scale", "nullable"]

#: Name characters: anything but NUL and surrogates, which the grammar
#: refuses. At most 8 of them of at most 4 bytes, on a path of at most
#: MAX_SHREDDING_DEPTH names, stays under MAX_PATH_NAME_BYTES.
_NAME = st.text(
    alphabet=st.characters(exclude_categories=("Cs",), exclude_characters="\x00"),
    min_size=1,
    max_size=8,
)


@st.composite
def _decimal(draw):
    type_, largest = draw(st.sampled_from(sorted(DECIMAL_TYPES.items())))
    precision = draw(st.integers(1, largest))
    return {
        "type": type_,
        "precision": precision,
        "scale": draw(st.integers(0, precision)),
    }


@st.composite
def _node(draw, depth: int, budget: list[int]):
    """A sound node at ``depth``. ``budget`` counts the fields and arrays
    still allowed, shared over the whole declaration."""
    containers = depth < MAX_SHREDDING_DEPTH and budget[0] > 4
    kind = draw(
        st.sampled_from(["object", "array", "leaf"] if containers else ["leaf"])
    )
    if kind == "object":
        seen: set[str] = set()
        fields = []
        for _ in range(draw(st.integers(1, 3))):
            name = draw(_NAME.filter(lambda n: n.lower() not in seen))
            seen.add(name.lower())
            budget[0] -= 1
            fields.append({"name": name, **draw(_node(depth + 1, budget))})
        return {"type": "object", "fields": fields}
    if kind == "array":
        budget[0] -= 1
        return {"type": "array", "element": draw(_node(depth + 1, budget))}
    leaf = draw(st.sampled_from(["variant", "decimal", *sorted(PRIMITIVE_TYPES)]))
    return draw(_decimal()) if leaf == "decimal" else {"type": leaf}


@st.composite
def _sound(draw):
    # A fresh budget per declaration: one shared across examples would make
    # the generator depend on the examples before it.
    return draw(_node(0, [60]))


def _nodes(decl, out=None):
    """Every node of a declaration, depth first."""
    out = [] if out is None else out
    out.append(decl)
    if decl["type"] == "object":
        for f in decl["fields"]:
            _nodes(f, out)
    elif decl["type"] == "array":
        _nodes(decl["element"], out)
    return out


def _objects(decl):
    return [n for n in _nodes(decl) if n["type"] == "object"]


def _break(draw, decl):
    """Break exactly one rule of ``decl`` in place; returns the rule."""
    nodes = _nodes(decl)
    target = draw(st.sampled_from(nodes))
    objects = _objects(decl)
    rule = draw(
        st.sampled_from(
            [
                "unknown type", "no type", "unknown key", "bad decimal", "too deep",
                *(["case collision", "empty name", "NUL", "surrogate", "name bytes", "no fields"]
                  if objects else []),
            ]
        )
    )  # fmt: skip
    if rule == "unknown type":
        target["type"] = draw(st.text(max_size=12).filter(
            lambda t: t not in PRIMITIVE_TYPES | set(DECIMAL_TYPES) | {"object", "array", "variant"}
        ))  # fmt: skip
    elif rule == "no type":
        target["type"] = draw(st.one_of(st.none(), st.integers(), st.booleans()))
    elif rule == "unknown key":
        # Unknown to THIS node: another type's key (a decimal's "fields",
        # an object's "precision") or "name" off a field is one too.
        allowed = {"type", *_TYPE_KEYS.get(target["type"], ())}
        if "name" in target:
            allowed.add("name")
        key = draw(
            (st.sampled_from(_GRAMMAR_KEYS) | st.text(min_size=1, max_size=6)).filter(
                lambda k: k not in allowed
            )
        )
        target[key] = 1
    elif rule == "bad decimal":
        keep = {k: v for k, v in target.items() if k == "name"}
        target.clear()
        type_, largest = draw(st.sampled_from(sorted(DECIMAL_TYPES.items())))
        bad = draw(
            st.sampled_from(
                [
                    (largest + 1, 0),
                    (0, 0),
                    (5, 6),
                    (5, -1),
                    (18.0, 2),
                    ("18", 2),
                    (True, 0),
                    (5, None),
                ]
            )
        )
        target.update(keep, type=type_, precision=bad[0], scale=bad[1])
    elif rule == "too deep":
        chain = {"type": "string"}
        for _ in range(MAX_SHREDDING_DEPTH + 1):
            chain = {"type": "array", "element": chain}
        keep = {k: v for k, v in target.items() if k == "name"}
        target.clear()
        target.update(keep, **chain)
    else:
        obj = draw(st.sampled_from(objects))
        first = obj["fields"][0]
        if rule == "case collision":
            # An ASCII pair, whose collision the client decides: a pair
            # with a capital sigma, or past the server's Unicode, is the
            # server's (validate_shredding). Otherwise an exact duplicate.
            name = first["name"]
            other = name.swapcase() if name.isascii() else name
            obj["fields"].append({"name": other, "type": "string"})
        elif rule == "empty name":
            first["name"] = ""
        elif rule == "NUL":
            first["name"] += "\x00"
        elif rule == "surrogate":
            first["name"] += draw(st.sampled_from(["\ud800", "\udc00"]))
        elif rule == "name bytes":
            first["name"] += "x" * MAX_PATH_NAME_BYTES
        else:
            obj["fields"] = draw(st.sampled_from([[], None, {"a": {"type": "string"}}]))
    return rule


@given(_sound())
def test_a_sound_declaration_is_accepted(decl):
    validate_shredding(decl, column="v")


@given(st.data())
def test_a_declaration_breaking_one_rule_is_refused(data):
    decl = data.draw(_sound())
    rule = _break(data.draw, decl)
    try:
        validate_shredding(decl, column="v")
    except ValidationError as e:
        assert e.message.startswith(
            "variant column 'v' has an invalid type_params.shredding: $"
        )
    else:
        raise AssertionError(f"accepted a declaration with {rule}: {decl!r}")


def _minimal(type_: str) -> dict:
    """The smallest sound node of ``type_``."""
    if type_ == "object":
        return {"type": "object", "fields": [{"name": "a", "type": "string"}]}
    if type_ == "array":
        return {"type": "array", "element": {"type": "string"}}
    if type_ in DECIMAL_TYPES:
        return {"type": type_, "precision": 1, "scale": 0}
    return {"type": type_}


@pytest.mark.parametrize("as_field", [False, True], ids=["root", "field"])
@pytest.mark.parametrize(
    "type_", sorted({"object", "array", "variant", *PRIMITIVE_TYPES, *DECIMAL_TYPES})
)
def test_every_key_a_node_may_not_have_is_unknown_to_it(type_, as_field):
    """Exhaustive over node types, where the generator above samples: each
    key of the grammar this node may not have — another type's, or
    ``name`` off a field — is an unknown key, refused at the node."""
    allowed = {"type", *_TYPE_KEYS.get(type_, ()), *(["name"] if as_field else [])}
    for key in sorted(set(_GRAMMAR_KEYS) - allowed):
        node = {**_minimal(type_), key: 1}
        decl, path = node, "$"
        if as_field:
            decl = {"type": "object", "fields": [{"name": "f", **node}]}
            path = "$.f"
        with pytest.raises(ValidationError) as ei:
            validate_shredding(decl)
        assert ei.value.message == (
            f"invalid type_params.shredding: {path} has an unknown key '{key}'"
        )


_JSON = st.recursive(
    st.none()
    | st.booleans()
    | st.integers()
    | st.floats(allow_nan=False)
    | st.text(max_size=80),
    lambda inner: (
        st.lists(inner, max_size=4)
        | st.dictionaries(
            st.sampled_from(
                ["type", "fields", "element", "name", "precision", "scale", "x"]
            )
            | st.text(max_size=80),
            inner,
            max_size=4,
        )
    ),
    max_leaves=40,
)


@given(_JSON)
def test_arbitrary_json_is_refused_only_with_a_bounded_validation_error(value):
    try:
        validate_shredding(value)
    except ValidationError as e:
        assert len(e.message) <= 2048
    # Anything else escapes and fails the test.


@given(_sound(), st.booleans())
def test_a_declaration_survives_the_marker(decl, nullable):
    field = variant_field("v", decl, nullable=nullable)
    [col] = schema_to_column_defs(pa.schema([field]))
    assert col == {
        "name": "v",
        "type": "variant",
        "nullable": nullable,
        "type_params": {"shredding": decl},
    }
    # The wire body is plain JSON that round-trips unchanged.
    assert json.loads(json.dumps(col)) == col


def _shuffle_keys(draw, node):
    if isinstance(node, dict):
        keys = draw(st.permutations(list(node)))
        return {k: _shuffle_keys(draw, node[k]) for k in keys}
    if isinstance(node, list):
        return [_shuffle_keys(draw, v) for v in node]
    return node


@given(st.data())
def test_the_marker_depends_on_key_order_not_at_all(data):
    decl = data.draw(_sound())
    shuffled = _shuffle_keys(data.draw, decl)
    assert (
        variant_field("v", decl).metadata[VARIANT_FIELD_KEY]
        == variant_field("v", shuffled).metadata[VARIANT_FIELD_KEY]
    )
