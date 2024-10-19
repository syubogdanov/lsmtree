from lsmtree.domain.dtypes.uint32 import Uint32


class Bytes32(bytes):
    """32-битный `bytes`."""

    min_len: int = Uint32.min
    max_len: int = Uint32.max
