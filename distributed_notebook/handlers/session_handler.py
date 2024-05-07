import jupyter_server.services.sessions.handlers as jupyter_session_handlers
from jupyter_server.utils import ensure_async
from tornado import web
from jupyter_client.kernelspec import NoSuchKernel
from jupyter_server.auth.decorator import authorized
from jupyter_server.utils import url_path_join
import jsonpatch
import sys 
import traceback
import json

from typing import List, Any

try:
    from jupyter_client.jsonutil import json_default
except ImportError:
    from jupyter_client.jsonutil import date_default as json_default

class DistributedSessionHandler(jupyter_session_handlers.SessionHandler):
    """
    Extends the jupyter_server session handler.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log.info("Created a new instance of DistributedSessionHandler.")

    @web.authenticated
    @authorized
    async def get(self, session_id):
        """Get the JSON model for a single session."""
        sm = self.session_manager
        
        self.log.info("Retrieving Session '%s' from %s." % (session_id, str(type(sm))))
        
        try:
            model = await sm.get_session(session_id=session_id)
        except web.HTTPError as httpError:
            self.log.error("HTTP %d %s: %s" % (httpError.status_code, httpError.reason, httpError.log_message))
            self.log.error(traceback.format_exc())
            raise ex 
        except Exception as ex:
            self.log.error("Unexpected exception encountered: %s" % str(ex))
            self.log.error(traceback.format_exc())
            raise ex 
        
        self.finish(json.dumps(model, default=json_default))
    
class DistributedSessionRootHandler(jupyter_session_handlers.SessionRootHandler):
    """
    Extends the jupyter_server root session handler with the ability to specify the Session ID.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.log.info("Created a new instance of DistributedSessionRootHandler.")
    
    @web.authenticated
    @authorized
    async def post(self) -> None:
        """
        Overrides the super class method to allow for specifying the Session ID.
        
        Creates a new session.
        """
        self.log.info("<<< USING CUSTOM SESSION POST HANDLER >>>")
        
        # (unless a session already exists for the named session)
        sm = self.session_manager
        self.log.info("Session Manager is of type %s." % str(type(self.session_manager)))

        model: dict[str, Any] = self.get_json_body()
        
        self.log.info("HTTP POST --> /api/sessions JSON body: %s" % str(model))
        
        if model is None:
            raise web.HTTPError(400, "No JSON data provided")

        if "notebook" in model:
            self.log.warning("Sessions API changed, see updated swagger docs")
            model["type"] = "notebook"
            if "name" in model["notebook"]:
                model["path"] = model["notebook"]["name"]
            elif "path" in model["notebook"]:
                model["path"] = model["notebook"]["path"]

        try:
            # There is a high chance here that `path` is not a path but
            # a unique session id
            path = model["path"]
        except KeyError as e:
            raise web.HTTPError(400, "Missing field in JSON data: path") from e

        try:
            mtype = model["type"]
        except KeyError as e:
            raise web.HTTPError(400, "Missing field in JSON data: type") from e

        session_id = model.get("id", None)
        name = model.get("name", None)
        kernel = model.get("kernel", {})
        resource_spec = model.get("resource_spec", {})
        kernel_name = kernel.get("name", None)
        kernel_id = kernel.get("id", None)
        
        if not session_id:
            self.log.debug("No session ID specified. Will generate session ID.")
        
        if not kernel_id:
            self.log.debug("No kernel ID specified. Will generate kernel ID.")

        if not kernel_id and not kernel_name:
            self.log.debug("No kernel specified, using default kernel")
            kernel_name = None

        exists = await ensure_async(sm.session_exists(path=path))
        if exists:
            s_model = await sm.get_session(path=path)
        else:
            try:
                # We need to be using our custom manager here, as it accepts the session ID and resource spec as keyword arguments.
                # The default Session Manager does not. 
                s_model = await sm.create_session(
                    path=path,
                    kernel_name=kernel_name,
                    kernel_id=kernel_id,
                    session_id=session_id, 
                    resource_spec = resource_spec,
                    name=name,
                    type=mtype,
                )
            except NoSuchKernel:
                msg = (
                    "The '%s' kernel is not available. Please pick another "
                    "suitable kernel instead, or install that kernel." % kernel_name
                )
                status_msg = "%s not found" % kernel_name
                self.log.warning("Kernel not found: %s" % kernel_name)
                self.set_status(501)
                self.finish(json.dumps({"message": msg, "short_message": status_msg}))
                return
            except Exception as e:
                raise web.HTTPError(500, str(e)) from e

        location = url_path_join(self.base_url, "api", "sessions", s_model["id"])
        self.set_header("Location", location)
        self.set_status(201)
        self.finish(json.dumps(s_model, default=json_default))

print("\nSetting default handlers for Distributed Session Handler now.")

# The handlers that we've overridden. 
# The keys are the class names (cls.__name__) of the handlers that we're overriding.
# The values are the class objects that are used to override the key class.
overrides = {
    "SessionHandler": DistributedSessionHandler,
    "SessionRootHandler": DistributedSessionRootHandler,
}

default_handlers: List[tuple] = []
for path, cls in jupyter_session_handlers.default_handlers:
    print("Path \"%s\" currently using handler \"%s\"" % (str(path), str(cls.__name__)), flush = True)
    if cls.__name__ in overrides:
        print("\tUsing modified handler for path \"%s\"" % path, flush = True)
        # Use the same named class from here if it exists
        default_handlers.append((path, overrides[cls.__name__]))
    else:
        print("\tSticking with DEFAULT handler %s for path \"%s\"" % (cls.__name__, path), flush = True)
        default_handlers.append((path, cls))

for i in range(0, len(jupyter_session_handlers.default_handlers)):
    jupyter_session_handlers.default_handlers[i] = default_handlers[i]
    
# Updated regexes to allow for arbitrary kernel IDs (not just UUIDs).
_session_id_regex = r"(?P<session_id>^[a-zA-Z0-9_-]{1,36}$)"
# Regex Explanation:
# • ^: Matches the start of the string.
# • [a-zA-Z0-9_-]: Matches any alphanumeric character, hyphen, or underscore.
# • {1,36}: Specifies the length of the string, allowing it to be between 1 and 36 characters long.
# • $: Matches the end of the string.

default_handlers = [
    (r"/api/sessions/%s" % _session_id_regex, DistributedSessionHandler),
    (r"/api/sessions", DistributedSessionRootHandler),
]