class Uint32(int):
    """32-битное целое число."""

    bytes: int = 4

    min: int = 0
    max: int = 2 ** 32 - 1
