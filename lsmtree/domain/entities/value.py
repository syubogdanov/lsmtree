from typing import Annotated

from lsmtree.utils.limits import MAX_UINT32
from lsmtree.utils.typing import Length


Value = Annotated[bytes, Length(max=MAX_UINT32)]
