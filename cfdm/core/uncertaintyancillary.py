from . import abstract


class Uncertainty(abstract.PropertiesData):
    """An uncertainty ancillary construct of the CF data model.

    TODOU (copy from appendix I when merged)

    .. versionadded:: (cfdm) NEXTVERSION

    """

    @property
    def construct_type(self):
        """Return a description of the construct type.

        .. versionadded:: (cfdm) NEXTVERSION

        :Returns:

            `str`
                The construct type.

        **Examples**

        >>> f = {{package}}.{{class}}()
        >>> f.construct_type
        'uncertainty_ancillary'

        """
        return "uncertainty_ancillary"
