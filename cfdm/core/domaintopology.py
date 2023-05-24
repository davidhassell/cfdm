from . import abstract


class DomainTopology(abstract.PropertiesData):
    """TODOUGRID.

    .. versionadded:: (cfdm) TODOUGRIDVER

    """

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
