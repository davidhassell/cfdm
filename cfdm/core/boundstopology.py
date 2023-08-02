from . import abstract


class BoundsTopology(abstract.PropertiesData):
    """A domain topology construct of the CF data model.

    TODOUGRIDVER

    See CF Appendix I "The CF Data Model".

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
        'bounds_topology'

        """
        return "bounds_topology"
