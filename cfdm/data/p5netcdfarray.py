import logging
from contextlib import nullcontext

from . import abstract
from .locks import netcdf_lock
from .mixin import IndexMixin
from .netcdfindexer import netcdf_indexer

logger = logging.getLogger(__name__)


class P5netcdfArray(IndexMixin, abstract.FileArray):
    """A netCDF array accessed with `p5netcdf`.

    .. versionadded:: (cfdm) NEXTVERSION

    """

#    def __init__(
#        self,
#        filename=None,
#        address=None,
#        dtype=None,
#        shape=None,
#        mask=True,
#        unpack=True,
#        attributes=None,
#        storage_protocol=None,
#        storage_options=None,
#            backend=None,
#        variable=None,
#        source=None,
#        copy=True,
#    ):
#        """**Initialisation**
#
#        :Parameters:
#
#            filename: (sequence of `str`), optional
#                The location of the dataset containing the array.
#
#            address: (sequence of `str`), optional
#                How to find the array in the dataset.
#
#            dtype: `numpy.dtype`, optional
#                The data type of the array. May be `None` if is not
#                known. This may differ from the data type of the
#                array in the
#        
#            shape: `tuple`, optional
#                The shape of the dataset array.
#
#            {{init mask: `bool`, optional}}
#
#            {{init unpack: `bool`, optional}}
#
#            {{init attributes: `dict` or `None`, optional}}
#
#                If *attributes* is `None`, the default, then the
#                attributes will be set from those in the dataset
#                during the first `__getitem__` call.
#
#            {{init storage_protocol: `None` or `str`, optional}}
#
#
#            {{init storage_options: `dict` or `None`, optional}}
#
#            variable: optional
#                An open dataset variable object. Setting *variable*
#                does not replace the need for the *filename* and
#                *address* parameters, instead it complements them by
#                allowing faster data access.
#
#            {{init source: optional}}
#
#            {{init copy: `bool`, optional}}
#
#        """
#        super().__init__(source=source, copy=copy)
#
#        if source is not None:
#            try:
#                shape = source._get_component("shape", None)
#            except AttributeError:
#                shape = None
#
#            try:
#                filename = source._get_component("filename", None)
#            except AttributeError:
#                filename = None
#
#            try:
#                address = source._get_component("address", None)
#            except AttributeError:
#                address = None
#
#            try:
#                dtype = source._get_component("dtype", None)
#            except AttributeError:
#                dtype = None
#
#            try:
#                mask = source._get_component("mask", True)
#            except AttributeError:
#                mask = True
#
#            try:
#                unpack = source._get_component("unpack", True)
#            except AttributeError:
#                unpack = True
#
#            try:
#                attributes = source._get_component("attributes", None)
#            except AttributeError:
#                attributes = None
#
#            try:
#                storage_protocol = source._get_component(
#                    "storage_protocol", None
#                )
#            except AttributeError:
#                storage_protocol = None
#
#            try:
#                storage_options = source._get_component(
#                    "storage_options", None
#                )
#            except AttributeError:
#                storage_options = None
#
#            try:
#                backend = source._get_component("backend", None)
#            except AttributeError:
#                backend = None
#
#            try:
#                variable = source._get_component("variable", None)
#            except AttributeError:
#                variable = None
#
#        if shape is not None:
#            self._set_component("shape", shape, copy=False)
#
#        if filename is not None:
#            self._set_component("filename", filename, copy=False)
#
#        if address is not None:
#            self._set_component("address", address, copy=False)
#
#        self._set_component("dtype", dtype, copy=False)
#        self._set_component("mask", bool(mask), copy=False)
#        self._set_component("unpack", bool(unpack), copy=False)
#
#        if storage_protocol is not None:
#            self._set_component(
#                "storage_protocol", storage_protocol, copy=False
#            )
#
#        if storage_options is not None:
#            self._set_component("storage_options", storage_options, copy=copy)
#
#        if attributes is not None:
#            self._set_component("attributes", attributes, copy=copy)
#
#        if variable is not None:
#            if backend is None:
#                backend = variable.backend
#                
#            self._set_component("variable", variable, copy=False)
#
#        # By default, close the netCDF file after data array access
#        self._set_component("close", True, copy=False)
#
#        self._set_component("backend", backend, copy=False)

    @property
    def _lock(self):
        """Return the lock used for netCDF file access.

        TODO Returns a lock object that prevents concurrent reads of netCDF
        files, which are not currently supported by `h5netcdf` with
        the `h5py` backend.

        .. versionadded:: (cfdm) 1.11.2.0

        """        
        match self.get_backend(None):
            case "netCDF4" | "h5py" | None:
                return netcdf_lock
            case _:
                return nullcontext()
                
    def _attributes(self, var):
        """Get the variable attributes.

        If the attributes have not been set, then they are retrieved
        from the *var* and cached for fast future access.

        .. versionadded:: (cfdm) 1.12.0.0

        .. seealso:: `get_attributes`

        :Parameters:

            var: `p5netcdf.Variable`
                The variable.

        :Returns:

            `dict`
                The attributes. The returned attributes are not a copy
                of the cached dictionary.

        """
        attributes = self._get_component("attributes", None)
        if attributes is None:
            attributes = var.attrs.copy()
            self._set_component("attributes", attributes, copy=False)

        return attributes

    def _get_array(self, index=None):
        """Returns a subspace of the dataset variable.

        The subspace is defined by the `index` attributes, and is
        applied with `cfdm.netcdf_indexer`.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `__array__`, `index`

        :Parameters:

            {{index: `tuple` or `None`, optional}}

        :Returns:

            `numpy.ndarray`
                The subspace.

        """
        if index is None:
            index = self.index()

        with self._lock:
            dataset = None
            variable = self.get_variable(None)
            if variable is None:                
                dataset, path = self.open()
                variable = dataset[path]
                
            # Get the data, applying masking and scaling as required.
            array = netcdf_indexer(
                variable,
                mask=self.get_mask(),
                unpack=self.get_unpack(),
                always_masked_array=False,
                orthogonal_indexing=True,
                attributes=self._attributes(variable),
                copy=False,
            )
            array = array[index]

            
            if dataset is not None:
                self.close(dataset)
            else:
                dataset = variable.root
                if dataset.is_local():
                    self.close(dataset)

        return array

    def close(self, dataset):
        """Close the dataset containing the data.

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            dataset: `p5netcdf.File`
                The netCDF dataset to be closed.

        :Returns:

            `None`

        """
        if self._get_component("close"):
            dataset.close()

    def open(self, **kwargs):
        """Return a dataset object and address.

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            kwargs: optional
                Extra keyword arguments to `p5netcdf.File`.

        :Returns:

            (`p5netcdf.File`, `str`)
                The open file object, and the address of the data
                within the file.

        """
        from cfdm import p5netcdf

        return super().open(
            p5netcdf.File, mode="r", backend=self.get_backend(None), **kwargs
        )

