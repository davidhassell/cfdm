import numpy as np

from .abstract import Subarray


class NodesBoundsSubarray(Subarray):
    """TODOUGRID A subarray of an array compressed by gathering.

    A subarray describes a unique part of the uncompressed array.

    See CF section 8.2. "Lossless Compression by Gathering".

    .. versionadded:: (cfdm) 1.11.0.0

    """

    def __init__(
        self,
        data=None,
        indices=None,
        shape=None,
        node_coordinates=None,
        start_index=0,
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

            node_coordinatesd: array_like
                TODOUGRID Indices of the uncompressed subarray for the
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
            compressed_dimensions={0: (0,), 1: (1,)},
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
                start_index = source._get_component(
                    "start_index", 0
                )
            except AttributeError:
                 start_index = 0

        self._set_component(
            "start_index", start_index, copy=False
        )
        if node_coordinates is not None:
            self._set_component(
                "node_coordinates", node_coordinates, copy=False
            )

    def __getitem__(self, indices):
        """Return a subspace of the uncompressed data.

        x.__getitem__(indices) <==> x[indices]

        Returns a subspace of the uncompressed data as an independent
        numpy array.

        .. versionadded:: (cfdm) 1.11.0.0

        """

        connectivity_indices = self._select_data()
        if np.ma.is_MA(connectivity_indices):
            node_indices = connectivity_indices.compressed()
            u = np.ma.masked_all(self.shape, dtype=self.dtype)
            u[~connectivity_indices.mask] = self._select_nodes(node_indices)
        else:
            node_indices = connectivity_indices.flatten()
            u = self._select_nodes(node_indices)
            u = u.reshape(self.shape)
                    
        if indices is Ellipsis:
            return u

        return u[indices]

    def _select_nodes(self, indices):
        """TODOUGRID Select interpolation parameter values.
        
        TODOUGRID Selects interpolation parameter values for this interpolation
        subarea.
        
        .. versionadded:: (cfdm) 1.11.0.0
        
        .. seealso:: `_select_data`
        
        :Returns:
        
           `numpy.ndarray`
               TODOUGRID The values of the interpolation parameter array that
               correspond to this interpolation subarea.
        
        """
        start_index = self.start_index
        if start_index:
            indices = indices - stdart_index
        
        return self._asanyarray(self.node_coordinates,
                                indices,
                                check_mask=False)

    @property
    def dtype(self):
        """The data-type of the uncompressed data.

        .. versionadded:: (cfdm) 1.11.0.0

        """
        return self.nodes_coordinates.dtype

    @property
    def start_index(self):
        """TODOUGRID Indices of the uncompressed subarray for the compressed data.

        .. versionadded:: (cfdm) 1.11.0.0

        """
        return self._get_component("start_index")

    @property
    def node_coordinates(self):
        """TODOUGRIDIndices of the uncompressed subarray for the compressed data.

        .. versionadded:: (cfdm) 1.11.0.0

        """
        return self._get_component("node_coordinates")
