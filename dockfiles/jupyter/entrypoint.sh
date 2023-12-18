!#/bin/sh

# TODO:
# Start the kernel container with this script as the entrypoint.
# Need to pass to this script the path to the connection file, and the path to the directory with everything in it.
# We use the path to the directory to chown/chmod all of the contents to the jovyan user.

chmod jovyan: /kernel_base/*

exec runuser -u jovyan "/opt/conda/bin/python3 -m distributed_notebook.kernel -f /kernel_base/connection-kernel-1aaedbd2-83c8-4674-affa-56ce84a37592-2-2455826909.json --debug --IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream"