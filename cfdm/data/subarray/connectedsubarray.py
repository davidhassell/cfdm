import numpy as np

from .abstract import Subarray


class ConnectedSubarray(Subarray):
    """ TODOUGRID A subarray of an array compressed by gathering.

    A subarray describes a unique part of the uncompressed array.

    See CF section 8.2. "Lossless Compression by Gathering".

    .. versionadded:: (cfdm) TODOUGRIDVER

    """

    def __init__(
        self,
        data=None,
        indices=None,
        shape=None,
        compressed_dimensions=None,
            node_coordinates=None,
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
                  ``{2: (2,)}``

                *Parameter example:*
                  ``{1: (1, 2)}``

                *Parameter example:*
                  ``{0: (0, 1, 2)}``

            uncompressed_indices: `tuple`
                Indices of the uncompressed subarray for the
                compressed data.

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
                node_coordinates = source._get_component(
                    "node_coordinates", None
                )
            except AttributeError:
                node_coordinates = None

            try:
                start_index = source._get_component("start_index", None)
            except AttributeError:
                start_index = None

        if node_coordinates is not None:
            self._set_component(
                "node_coordinates", node_coordinates, copy=False
            )

        if start_index is None:
            start_index = 0

        self._set_component("start_index", start_index, copy=False)
        
    def __getitem__(self, indices):
        """Return a subspace of the uncompressed data.

        x.__getitem__(indices) <==> x[indices]

        Returns a subspace of the uncompressed data as an independent
        numpy array.

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        connectivity = self._select_data(check_mask=True)
        coords = self._asanyarray(self.node_coordinates, check_mask=False)

        start_index = self.start_index
        if start_index:
            connectivity -= start_index
        
        if np.ma.isMA(connectivity):
            # connectivity = connectivity.flatten()
            # mask = np.ma.getmaskarray(connectivity)
            # coords = coords[np.ma.compressed(connectivity}]
            # u = np.ma.masked_all(self.shape, dtype=self.dtype)
            # u[~mask] = coords
            mask = connectivity.mask
            connectivity = np.filled(connectivity, 0)
            coords = coords[connectivity.flatten()]
            u = coords.reshape(self.shape)
            u = np.ma.array(u, mask=mask)
        else:
            coords = coords[connectivity.flatten()]            
            u = coords.reshape(self.shape)
            
#        j, i = np.where(~np.ma.getmaskarray(coords))
#        u[j, i] = coords[np.unique(connectivity[j, i], return_inverse=True)[1]]

        if indices is Ellipsis:
            return u

        return u[indices]

    @property
    def dtype(self):
        """The data-type of the uncompressed data.

        .. versionadded:: (cfdm)  TODOUGRIDVER

        """
        return self.data.dtype

    @property
    def node_coordinates(self):
        """TODOUGRID

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        return self._get_component("node_coordinates")

    @property
    def start_index(self):
        """TODOUGRID

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        return self._get_component("start_index")
