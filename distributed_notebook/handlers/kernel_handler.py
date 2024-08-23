import jupyter_server.services.kernels.handlers as jupyter_kernel_handlers
from jupyter_server.services.kernels.websocket import KernelWebsocketHandler
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
    async def delete(self, kernel_id):
        """Remove a kernel."""
        self.log.info(f"Received 'DELETE' operation targeting kernel \"{kernel_id}\"")
        km = self.kernel_manager
        await ensure_async(km.shutdown_kernel(kernel_id))
        self.set_status(204)
        self.finish()
        
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
            raise httpError 
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
        print("\tUsing modified handler %s in place of default handler %s for path \"%s\"" % (overrides[cls.__name__].__name__, cls.__name__, path), flush = True)
        # Use the same named class from here if it exists
        default_handlers.append((path, overrides[cls.__name__]))
    else:
        print("\tSticking with DEFAULT handler %s for path \"%s\"" % (cls.__name__, path), flush = True)
        default_handlers.append((path, cls))

for i in range(0, len(jupyter_kernel_handlers.default_handlers)):
    jupyter_kernel_handlers.default_handlers[i] = default_handlers[i]

# -----------------------------------------------------------------------------
# URL to handler mappings
# -----------------------------------------------------------------------------

# Updated regexes to allow for arbitrary kernel IDs (not just UUIDs).
_kernel_action_regex = r"(?P<action>restart|interrupt)"
_kernel_id_regex = r"(?P<kernel_id>[a-zA-Z0-9_-]{1,36})"
# Regex Explanation:
# • [a-zA-Z0-9_-]: Matches any alphanumeric character, hyphen, or underscore.
# • {1,36}: Specifies the length of the string, allowing it to be between 1 and 36 characters long.

jupyter_kernel_handlers.default_handlers.extend([
    (r"/api/kernels", jupyter_kernel_handlers.MainKernelHandler),
    (r"/api/kernels/%s" % _kernel_id_regex, DistributedKernelHandler),
    (
        rf"/api/kernels/{_kernel_id_regex}/{_kernel_action_regex}",
        jupyter_kernel_handlers.KernelActionHandler,
    ),
    (r"/api/kernels/%s/channels" % _kernel_id_regex, KernelWebsocketHandler),
])

_kernel_id_regex = r"(?P<kernel_id>^[a-zA-Z0-9_-]{1,36}$)"
url = r"/api/kernels/%s" % _kernel_id_regex
