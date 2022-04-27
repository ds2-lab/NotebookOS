PyObject *
_wrap__smr_smr_NewBytes(PyObject * PYBINDGEN_UNUSED(dummy), PyObject *args, PyObject *kwargs)
{
    PyObject *py_retval;
    uint64_t retval;
    Py_buffer buffer;
    int len;
    const char *keywords[] = {"bytes", "len", NULL};
    char* bytes;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, (char *) "y*i", (char **) keywords, &buffer, &len)) {
        return NULL;
    }
    bytes = (char*)buffer.buf;
    retval = smr_NewBytes(bytes, len);
    if (PyErr_Occurred()) {
        return NULL;
    }
    py_retval = Py_BuildValue((char *) "L", retval);
    return py_retval;
}