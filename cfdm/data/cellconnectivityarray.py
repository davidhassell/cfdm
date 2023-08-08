from .abstract import ConnectivityArray


class CellConnectivityArray(ConnectivityArray):
    """An underlying UGRID connectivity array.

    See CF section 5.9 "Mesh Topology Variables".

    .. versionadded:: (cfdm) TODOUGRIDVER

    """

    def __init__(
        self, connectivity=None, start_index=0, source=None, copy=True
    ):
        """**Initialisation**

        :Parameters:

            connectivity: array_like
                TODOUGRID

            start_index: `int`, optional
                TODOUGRID

            {{init source: optional}}

            {{init copy: `bool`, optional}}

        """
        super().__init__(
            connectivity=connectivity,
            start_index=start_index,
            compression_type="cell connectivity",
            source=source,
            copy=copy,
        )
