from .subarray import Subarray


class MeshSubarray(Subarray):
    """A subarray of an arry defined by a UGRID connectivity variable.

    .. versionadded:: (cfdm) TODOUGRIDVER

    """

    def __init__(
        self,
        data=None,
        indices=None,
        shape=None,
        compressed_dimensions=None,
        dtype=None,
        start_index=None,
        source=None,
        copy=True,
        context_manager=None,
    ):
        """**Initialisation**

        :Parameters:

            data: array_like
                A 2-d integer array that contains zero-based indices
                that identifies UGRID nodes for each cell, as found in
                a UGRID connectivty variable. This array contains the
                indices for all subarrays.

            indices: `tuple` of `slice`
                The indices of *data* that define this subarray.

            shape: `tuple` of `int`
                The shape of the uncompressed subarray.

            {{init compressed_dimensions: `dict`}}

            {{start_index: `int`}}

            {{init source: optional}}

            {{init copy: `bool`, optional}}

            context_manager: function, optional
                A context manager that provides a runtime context for
                the conversion of data defined by *data* to a `numpy`
                array.

        """
        super().__init__(
            data=data,
            indices=indices,
            shape=shape,
            compressed_dimensions=compressed_dimensions,
            source=source,
            copy=copy,
            context_manager=context_manager,
        )

        if source is not None:
            try:
                start_index = source.get_start_index(None)
            except AttributeError:
                start_index = None

            try:
                dtype = source._get_component("dtype", None)
            except AttributeError:
                dtype = None

        if start_index is not None:
            self._set_component("start_index", start_index, copy=False)

        if dtype is not None:
            self._set_component("dtype", dtype, copy=False)

    @property
    def dtype(self):
        """The data-type of the uncompressed data.

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        return self.data.dtype

    @property
    def start_index(self):
        """The start index of values in the compressed data.

        Either ``0`` or ``1``.

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        return self._get_component("start_index")
