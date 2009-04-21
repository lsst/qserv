#include "Python.h"
#include <iostream>

#define NO_XRD 1

#ifndef NO_XRD
#include "XrdPosixExtern.hh"
#endif


#ifndef NO_XRD
static PyObject* xrdOpen(PyObject *self, PyObject *args) {
    const char *filePath;
    int fh;

    if (!PyArg_ParseTuple(args, "s", &filePath))
        return NULL;
    fh = XrdPosix_Open(filePath, O_RDWR);

    return Py_BuildValue("i", fh);
}

static PyObject* xrdWrite(PyObject *self, PyObject *args) {
    int fh;
    const char *data;
    int size;
    long long result;

    if (!PyArg_ParseTuple(args, "is#", &fh, &data, &size))
        return NULL;
    result = XrdPosix_Write(fh, int fildes, data, size);

    return Py_BuildValue("i", result);
}

static PyObject* xrdRead(PyObject *self, PyObject *args) {
    int fh;
    char *data;
    char* finaldata = 0;
    int size = 0;
    int chunksize = 200; // set small for now.
    long long result = chunksize;
    PyObject* obj;

    data = malloc(chunksize);
    assert(data);
    if (!PyArg_ParseTuple(args, "i", &fh))
        return NULL;
    while(result == chunksize) {
	// Might be possible to optimize if I think harder.
	result = XrdPosix_Read(fh, data, chunksize); // perform read
	finaldata = realloc(finaldata, size + result); // make room
	memcpy(finaldata+size, data, result); // copy result
	size += result; // update size
    }
    free(data);
    obj =  Py_BuildValue("s", finaldata); 
    free(finaldata);
    return obj;
}


#else // ------------------------------ Fake placeholder implemenation
static PyObject* xrdOpen(PyObject *self, PyObject *args) {
    std::cout << "xrd openfile (50)" << std::endl;
    return Py_BuildValue("i",  50);
}

static PyObject* xrdWrite(PyObject *self, PyObject *args) {
    int fh;
    const char *data;
    int size;
    long long result;

    if (!PyArg_ParseTuple(args, "is#", &fh, &data, &size))
        return NULL;
    std::cout << "xrd write to descriptor " <<  fh << " \"" 
	      << std::string(data, size) << std::endl;
    return Py_BuildValue("i", size);
}

static PyObject* xrdRead(PyObject *self, PyObject *args) {
    int fh;
    std::cout << "xrd read: faked" << std::endl;
    return Py_BuildValue("s",  "fake read results");
}

static PyObject* xrdClose(PyObject *self, PyObject *args) {
    int fh;
    if (!PyArg_ParseTuple(args, "i", &fh))
        return NULL;
    std::cout << "xrd close file" << fh << std::endl;
    return Py_BuildValue("i",  0);
}

#endif


// Python method table
static PyMethodDef xrdFileMethods[] = {
    {"xrdOpen",  xrdOpen, METH_VARARGS,
     "Open an Xrd file for read-write.  Returns a (int) file handle."},
    {"xrdRead",  xrdRead, METH_VARARGS,
     "Read some bytes from an Xrd file."},
    {"xrdWrite",  xrdWrite, METH_VARARGS,
     "Write some bytes to an Xrd File."},
    {"xrdClose",  xrdClose, METH_VARARGS,
     "Close  an Xrd File."},
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

PyMODINIT_FUNC initxrdfile(void) {
    (void) Py_InitModule("xrdfile", xrdFileMethods);
}
