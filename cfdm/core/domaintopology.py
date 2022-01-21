from . import abstract


class DomainTopology(abstract.PropertiesData):
    """TODO.

    .. versionadded:: (cfdm) 1.10.0

    """

    @property
    def construct_type(self):
        """Return a description of the construct type.

        .. versionadded:: (cfdm) 1.10.0

        :Returns:

            `str`
                The construct type.

        **Examples:**

        >>> d = {{package}}.{{class}}()
        >>> d.construct_type
        'domain_topology'

        """
        return "domain_topology"
