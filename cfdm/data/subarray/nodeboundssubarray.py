import numpy as np

from .abstract import Subarray


class NodeBoundsSubarray(Subarray):
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
            compressed_dimensions={1: (1,)},
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
                start_index = source._get_component("start_index", 0)
            except AttributeError:
                start_index = 0

        self._set_component("start_index", start_index, copy=False)
        if node_coordinates is not None:
            self._set_component(
                "node_coordinates", node_coordinates, copy=False
            )

    def __getitem__(self, indices):
        """Return a subspace of the uncompressed data.

        x.__getitem__(indices) <==> x[indices]

        Returns a subspace of the uncompressed data as an independent
        numpy array.

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        node_connectivity = self._select_data()
        if np.ma.isMA(node_connectivity):
            node_indices = node_connectivity.compressed()
            u = np.ma.masked_all(self.shape, dtype=self.dtype)
            u[~node_connectivity.mask] = self._select_node_coordinates(
                node_indices
            )
        else:
            node_indices = node_connectivity.flatten()
            u = self._select_node_coordinates(node_indices)
            u = u.reshape(self.shape)

        if indices is Ellipsis:
            return u

        return u[indices]

    def _select_node_coordinates(self, node_indices):
        """TODOUGRID Select interpolation parameter values.

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `_select_data`

        :Parameters:

            node_indices: array_like
                Indices to the node coordinates array. Must be a 1-d
                integer array that supports fancy indexing. If
                *indices* are not zero-based then `start_index` will
                be substracted prior to their application.

        :Returns:

           `numpy.ndarray`
               The node coordiantes that correspond to the
               *node_indices*.

        """
        start_index = self.start_index
        if start_index:
            node_indices = node_indices - start_index

        return self._asanyarray(
            self.node_coordinates, node_indices, check_mask=False
        )

    @property
    def dtype(self):
        """The data-type of the uncompressed data.

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        return self.nodes_coordinates.dtype

    @property
    def start_index(self):
        """The base of the node indices.

        Either ``0`` or ``1``.

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        return self._get_component("start_index")

    @property
    def node_coordinates(self):
        """The coordinates representing the node locations.

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        return self._get_component("node_coordinates")
