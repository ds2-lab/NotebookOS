from entrypoints import EntryPoint

from traitlets.config import LoggingConfigurable
from traitlets.config import Unicode

class A(LoggingConfigurable):
    kernel_id: str = Unicode(None, allow_none=True)

ep = EntryPoint('a', '__main__', 'A')
cls = ep.load()
config = {
    'kernel_id': '123'
}
a = cls(**config)

print(a.kernel_id)