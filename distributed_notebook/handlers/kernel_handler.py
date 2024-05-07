import jupyter_server.services.kernels.handlers as jupyter_kernel_handlers
from jupyter_server.utils import ensure_async
from tornado import web
from jupyter_server.auth.decorator import authorized
import traceback
import json

from typing import List, Any

try:
    from jupyter_client.jsonutil import json_default
except ImportError:
    from jupyter_client.jsonutil import date_default as json_default
    
class DistributedKernelHandler(jupyter_kernel_handlers.KernelHandler):
    """
    Extends the jupyter_server kernel handler.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log.info("Created a new instance of DistributedKernelHandler.")
        
    @web.authenticated
    @authorized
    async def get(self, kernel_id):
        """Get a kernel model."""
        km = self.kernel_manager
        
        self.log.info("Using %s to retrieve kernel '%s'." % (type(km).__name__, kernel_id))
        
        try:
            model = await ensure_async(km.kernel_model(kernel_id))
        except web.HTTPError as httpError:
            self.log.error("HTTP %d %s: %s" % (httpError.status_code, httpError.reason, httpError.log_message))
            self.log.error(traceback.format_exc())
            raise ex 
        except Exception as ex:
            self.log.error("Unexpected exception encountered: %s" % str(ex))
            self.log.error(traceback.format_exc())
            raise ex 
        
        self.finish(json.dumps(model, default=json_default))

# The handlers that we've overridden. 
# The keys are the class names (cls.__name__) of the handlers that we're overriding.
# The values are the class objects that are used to override the key class.
overrides = {
    "KernelHandler": DistributedKernelHandler,
}

print("\nSetting default handlers for Distributed Kernel Handler now.")
default_handlers: List[tuple] = []
for path, cls in jupyter_kernel_handlers.default_handlers:
    print("Path \"%s\" currently using handler \"%s\"" % (str(path), str(cls.__name__)), flush = True)
    if cls.__name__ in overrides:
        print("\tUsing modified handler for path \"%s\"" % path, flush = True)
        # Use the same named class from here if it exists
        default_handlers.append((path, overrides[cls.__name__]))
    else:
        print("\tSticking with DEFAULT handler %s for path \"%s\"" % (cls.__name__, path), flush = True)
        default_handlers.append((path, cls))

for i in range(0, len(jupyter_kernel_handlers.default_handlers)):
    jupyter_kernel_handlers.default_handlers[i] = default_handlers[i]