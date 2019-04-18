from typing import Any, List, Tuple


def flatten_tuple_fields(t: Any, prefixes = None) -> List[str]:
    """
    flatten_tuple_fields takes in a potentially nested named tuple and returns
    a flattened list of its fields. For example, consider the following named
    tuple.

        class A(NamedTuple):
            x: int
            y: int

        class B(NamedTuple):
            z: int
            a1: A
            a2: A

        t = B(0, A(1, 2), A(3, 4))

    Then, `flatten_tuple_fields(t)` is `['z', 'a1.x', 'a1.y', 'a2.x', 'a2.y']`.
    """
    return _flatten_tuple_fields(t, [])


def flatten_tuple(t: Tuple) -> List[Any]:
    """
    flatten_tuple recursively flattens a nested tuple.

        >>> flatten_tuple((1, 2, (3, (4, 5)), 6))
        [1, 2, 3, 4, 5, 6]
    """
    values: List[Any] = []
    for x in t:
        if isinstance(x, tuple):
            values += flatten_tuple(x)
        else:
            values.append(x)
    return values


# See https://stackoverflow.com/a/2166841/3187068.
def _is_namedtuple_instance(x):
    b = type(x).__bases__
    if len(b) != 1 or b[0] != tuple:
        return False

    f = getattr(type(x), '_fields', None)
    if not isinstance(f, tuple):
        return False

    return all(type(n) == str for n in f)


def _flatten_tuple_fields(t: Any, prefixes: List[str]) -> List[str]:
    fields: List[str] = []
    for (field, x) in zip(t._fields, t):
        if _is_namedtuple_instance(x):
            fields += _flatten_tuple_fields(x, prefixes + [field])
        else:
            fields.append('.'.join(prefixes + [field]))
    return fields
