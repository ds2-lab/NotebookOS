This Docker image is used as the base for the Jupyter image.

It contains a newer version of Python. We create a separate base image so that we never have to fully rebuild Python when rebuilding the Jupyter image.