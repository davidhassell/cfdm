from .abstract import MeshArray
from .subarray import BoundsFromNodesSubarray


class BoundsFromNodesArray(MeshArray):
    """TODOUGRID An underlying gathered array.

    Create cell bounds from data as stored by a
    [face|ege|volum]_node_connectivity variable

    Compression by gathering combines axes of a multidimensional array
    into a new, discrete axis whilst omitting the missing values and
    thus reducing the number of values that need to be stored.

    The information needed to uncompress the data is stored in a "list
    variable" that gives the indices of the required points.

    See CF section 8.2 "Lossless Compression by Gathering".

    .. versionadded:: (cfdm) TODOUGRIDVER

    """

    def __new__(cls, *args, **kwargs):
        """Store subarray classes.

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        instance = super().__new__(cls)
        instance._Subarray = {"bounds from nodes": BoundsFromNodesSubarray}
        return instance

    def __init__(
        self,
        node_connectivity=None,
        shape=None,
        start_index=None,
        node_coordinates=None,
        source=None,
        copy=True,
    ):
        """**Initialisation**

        :Parameters:

            node_connectivity: array_like
                A 2-d integer array that maps the bounds positions of
                each cell to the corresponding mesh nodes, as found in
                a UGRID "edge_node_connection",
                "face_node_connection", or "volume_node_connection"
                variable.

            shape: `tuple`
                The shape of the bounds array.

            node_coordinates: array_like
                A 1-d array that contains a coordinate for each mesh
                node, as found in a UGRID "node_coordinates" variable.

            start_index: `int`, optional
                TODOUGRID

            {{init source: optional}}

            {{init copy: `bool`, optional}}

        """
        if shape is None and node_connectivity is not None:
            shape = node_connectivity.shape

        super().__init__(
            connectivity=node_connectivity,
            shape=shape,
            start_index=start_index,
            compressed_dimensions={1: (1,)},
            compression_type="bounds from nodes",
            source=source,
            copy=copy,
        )

        if source is not None:
            try:
                node_coordinates = source._get_component(
                    "node_coordinates", None
                )
            except AttributeError:
                node_coordinates = None

        if node_coordinates is not None:
            self._set_component(
                "node_coordinates", node_coordinates, copy=copy
            )

    @property
    def dtype(self):
        """The data-type of the uncompressed data.

        .. versionadded:: (cfdm) TODOUGRIDVER

        """
        return self.get_node_coordinates().dtype

    def conformed_data(self):
        """The compressed data and TODOUGRID connectivity indices.

        .. versionadded:: (cfdm) TODOUGRIDVER

        :Returns:

            `dict`
                The conformed gathered data, with the key ``'data'``;
                and the `tuple` of uncompressed indices with the key
                ``'uncompressed_indices'``.

        """
        out = super().conformed_data()
        out["node_coordinates"] = self.get_node_coordinates()
        return out

    def get_node_coordinates(self, default=ValueError()):
        """The coordinates representing the node locations.

        .. versionadded:: (cfdm) TODOUGRIDVER

        :Parameters:

            default: optional
                Return the value of the *default* parameter if node
                coordinates indices have not been set.

                {{default Exception}}

        :Returns:

                The node coordinates.

        """
        return self._get_component("node_coordinates", default=default)

    def to_memory(self):
        """Bring data on disk into memory.

        There is no change to data that is already in memory.

        .. versionadded:: (cfdm) TODOUGRIDVER

        :Returns:

            `{{class}}`
                A copy of the array with all of its data in memory.

        """
        a = super().to_memory()

        node_coordinates = self.get_node_coordinates(None)
        if node_coordinates is not None:
            try:
                a._set_component(
                    "node_coordinates",
                    node_coordinates.to_memory(),
                    copy=False,
                )
            except AttributeError:
                pass

        return a
