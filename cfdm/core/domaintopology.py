from . import abstract


class DomainTopology(abstract.Topology):
    """A domain topology construct of the CF data model.

    TODOUGRID

    See CF Appendix I "The CF Data Model".

    .. versionadded:: (cfdm) TODOUGRIDVER

    """

    def __init__(
        self,
        cell=None,
        properties=None,
        data=None,
        source=None,
        copy=True,
        _use_data=True,
    ):
        """**Initialisation**

        :Parameters:

            topologyTODOUGRID: `str`, optional
                Set the topology type that indicates which aspect of
                the mesh topology is represented by the construct.

                The topology type may also be set after initialisation
                with the `set_topology` method.

            {{init properties: `dict`, optional}}

                *Parameter example:*
                  ``properties={'long_name': 'face-face connectivity'}``

            {{init data: data_like, optional}}

            {{init source: optional}}

            {{init copy: `bool`, optional}}

        """
        super().__init__(
            properties=properties,
            source=source,
            data=data,
            copy=copy,
            _use_data=_use_data,
        )

        if source is not None:
            try:
                cell = source.get_cell(None)
            except AttributeError:
                cell = None

        if cell is not None:
            self.set_cell(cell)

    @property
    def construct_type(self):
        """Return a description of the construct type.

        .. versionadded:: (cfdm) TODOUGRIDVER

        :Returns:

            `str`
                The construct type.

        **Examples:**

        >>> d = {{package}}.{{class}}()
        >>> d.construct_type
        'domain_topology'

        """
        return "domain_topology"

    def del_cell(self, default=ValueError()):
        """Remove the cell type.

        {{cell type}}

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `get_cell`, `has_cell`, `set_cell`

        :Parameters:

            default: optional
                Return the value of the *default* parameter if the
                cell type has not been set.

                {{default Exception}}

        :Returns:

                The removed cell type.

        """
        return self._del_component("cell", default=default)

    def has_cell(self):
        """Whether the cell type has been set.

        {{cell type}}

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `del_cell`, `get_cell`, `set_cell`

        :Returns:

             `bool`
                True if the cell type has been set, otherwise False.

        """
        return self._has_component("cell")

    def get_cell(self, default=ValueError()):
        """Return the cell type.

        {{cell type}}

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `del_cell`, `has_cell`, `set_cell`

        :Parameters:

            default: optional
                Return the value of the *default* parameter if the
                cell type has not been set.

                {{default Exception}}

        :Returns:

                The value of the cell type.

        """
        return self._get_component("cell", default=default)

    def set_cell(self, cell):
        """Set the cell type type.

        {{cell type}}

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `del_cell`, `get_cell`, `has_cell`

        :Parameters:

            cell: `str`
                The value for the cell type.

        :Returns:

             `None`

        """
        cell_types = ("point", "edge", "face", "volume")
        if cell not in cell_types:
            raise ValueError(
                f"Can't set cell type of {cell!r}. "
                f"Must be one of {cell_types}"
            )

        self._set_component("cell", cell, copy=False)
