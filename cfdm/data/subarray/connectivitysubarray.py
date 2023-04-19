import numpy as np

from ...core.utils import cached_property
from .abstract import Subarray


class ConnectivitySubarray(Subarray):
    """A subarray of a compressed UGRID connectivity array.

    A subarray describes a unique part of the uncompressed array.

    See CF section 5.9 "Mesh Topology Variables".

    .. versionadded:: (cfdm) 1.10.0.0

    """

    def __init__(
        self,
        data=None,
        indices=None,
        shape=None,
        compressed_dimensions={},
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
                  ``{1: (1,)}``

            source: optional
                Initialise the subarray from the given object.

                {{init source}}

            copy: `bool`, optional
                If False then do not deep copy input parameters prior
                to initialisation. By default arguments are deep
                copied.

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

    def __getitem__(self, indices):
        """Return a subspace of the uncompressed data.

        x.__getitem__(indices) <==> x[indices]

        Returns a subspace of the uncompressed data as an independent
        numpy array.

        .. versionadded:: (cfdm) 1.10.0.0

        """
        from scipy.sparse import csc_array

        connectivity = self._select_data(check_mask=False) # only select on dim0
        
        shape = connectivity.shape
        if np.ma.is_masked(connectivity):
            indptr = shape[1] - np.ma.getmaskarray(connectivity).sum(axis=1)
            indptr = np.insert(indptr, 0, 0)
            connectivity = connectivity.compressed()
        else:
            indptr = np.full((shape[0] + 1,), shape[1])
            indptr[0] = 0
            connectivity = connectivity.flatten()
        
        indptr = np.cumsum(indptr, out=indptr)

        start_index = self.get_start_index()
        if start_index:
            connectivity -= start_index
            
        data = np.ones((connectivity.size,), bool)

        c = csc_array((data, connectivity, indptr))
        return c[:, self.indices[1])

    @cached_property
    def dtype(self):
        """The data-type of the uncompressed data.

        .. versionadded:: (cfdm) 1.10.0.0

        """
        return np.dtype(bool)
