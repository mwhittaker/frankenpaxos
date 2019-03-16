from enum import Enum
from typing import Any, Dict, List, NamedTuple, Tuple, Union

# A scalar protobuf value.
Scalar = Union[str, int, Enum]

# A protobuf value. Either a scalar, a list of scalars, or a message (Any).
Value = Union[Scalar, List[Scalar], Any]

# A protobuf message. Any is Value.
Message = Dict[str, Any]

def message_to_pbtext(d: Message, indent: int = 0) -> str:
    """
    Convert a dictionary representation of a protobuf into the ascii
    representation of the protobuf. For example, the following dict:

      {
          "f": 1,
          "leaderAddress": [
            {"host", "localhost", "port": 9000},
            {"host", "localhost", "port": 9001},
          ]
      }

    is converted into the following ascii:

      f: 1
      leaderAddress: {
          host: "localhost"
          port: 9000
      }
      leaderAddress: {
          host: "localhost"
          port: 9001
      }
    """
    # Flatten repeated fields in the proto.
    flattened: List[Tuple[str, Union[Scalar, Message]]] = []
    for (k, v) in d.items():
        if isinstance(v, list):
            flattened += [(k, x) for x in v]
        else:
            flattened.append((k, v))

    strings: List[str] = []
    for (k, v) in flattened:
        if isinstance(v, str):
            strings.append(f'{k}: "{v}"')
        elif isinstance(v, int):
            strings.append(f'{k}: {v}')
        elif isinstance(v, Enum):
            strings.append(f'{k}: {v.name}')
        else:
            # The value must be a message.
            substring = _indent(message_to_pbtext(v, indent + 2), indent + 2)
            strings.append(f'{k}: {{\n{substring}\n}}')
    return "\n".join(strings)

def _indent(s: str, n: int) -> str:
    padding = " " * n
    return padding + s.replace("\n", "\n" + padding)
