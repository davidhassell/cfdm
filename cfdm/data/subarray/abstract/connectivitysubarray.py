from .subarray import Subarray


class ConnectivitySubarray(Subarray):
    """A subarray TODOUGRID of an array compressed by subsampling.

    See CF section 8.3 "Lossy Compression by Coordinate Subsampling"
    and appendix J "Coordinate Interpolation Methods".

    .. versionadded:: (cfdm) TODOUGRIDVER

    """

    def __init__(
        self,
        data=None,
        indices=None,
        shape=None,
        compressed_dimensions=None,
        start_index=None,
        source=None,
        copy=True,
        context_manager=None,
    ):
        """**Initialisation**

        :Parameters:

            data: array_like
                The full compressed array spanning all subarrays, from
                which the elements for this subarray are defined by
                the *indices*.

            indices: `tuple` of `slice`
                The indices of *data* that define this subarray.

            shape: `tuple` of `int`
                The shape of the uncompressed subarray.

            compressed_dimensions: `dict`
                Mapping of compressed to uncompressed dimensions.

                A dictionary key is a position of a dimension in the
                compressed data, with a value of the positions of the
                corresponding dimensions in the uncompressed
                data. Compressed array dimensions that are not
                compressed must be omitted from the mapping.

                *Parameter example:*
                  ``{2: (2,)}`` TODOUGRID

            start_index: `int`
                TODOUGRID

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
            compressed_dimensions=compressed_dimensions, # TODOUGRID
            source=source,
            copy=copy,
            context_manager=context_manager,
        )

        if source is not None:
            try:
                start_index = source.get_start_index(None)
            except AttributeError:
                start_index = None

        if start_index is not None:
            self._set_component("start_index", start_index, copy=False)

    @property
    def dtype(self):
        """The data-type of the uncompressed data.

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        from ....functions import integer_dtype
        
        return integer_dtype(self.shape[0] - 1)

    @property
    def start_index(self):
        """The start index of values in the compressed data.

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        return self._get_component("start_index")
