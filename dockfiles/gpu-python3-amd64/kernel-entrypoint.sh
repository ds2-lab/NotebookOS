#!/bin/sh

# First, we need to fill-in the IPython configuration file.
sed "s/{{replica_id}}/$(echo $POD_NAME | cut -d \"-\" -f 7)/g" "/kernel-configmap/ipython_config.json"

ln -s /kernel-configmap/ipython_config.json /home/jovyan/.ipython/profile_default/ipython_config.json

# Next, execute the kernel.
/opt/conda/bin/python3 -m distributed_notebook.kernel -f $CONNECTION_FILE_PATH --debug --IPKernelApp.outstream_class=distributed_notebook.kernel.iostream.OutStream