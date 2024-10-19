class Uint1024(int):
    """1024-битное целое число."""

    bytes: int = 128

    min: int = 0
    max: int = 2 ** 1024 - 1
