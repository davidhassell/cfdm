from math import prod

from .abstract import PropertiesData


class UncertaintyAncillary(PropertiesData):
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

    @property
    def ndim(self):
        """The number of data dimensions.
        TODOU

        Only the data dimensions that correspond to a domain axis
        construct are included.

        .. versionadded:: (cfdm) TODOU

        .. seealso:: `data`, `has_data`, `shape`, `size`

        **Examples**

        >>> d.shape
        (1324,)
        >>> d.ndim
        1
        >>> f.size
        1324

        """
        return super().ndim // 2

    @property
    def shape(self):
        """A tuple of the data array's dimension sizes.
        TODOU
        Only the data dimension that corresponds to a domain axis
        construct is included.

        .. versionadded:: (cfdm) 1.11.0.0

        .. seealso:: `data`, `has_data`, `ndim`, `size`

        **Examples**

        >>> d.shape
        (1324,)
        >>> d.ndim
        1
        >>> d.size
        1324

        """
        shape = super().shape
        return shape[: len(shape) // 2]

    @property
    def size(self):
        """The number elements in the data.
        TODOU
        `size` is equal to the product of `shape`, that only includes
        the data dimension corresponding to a domain axis construct.

        .. versionadded:: (cfdm) TODOU

        .. seealso:: `data`, `has_data`, `ndim`, `shape`

        **Examples**

        >>> d.shape
        (1324,)
        >>> d.ndim
        1
        >>> d.size
        1324

        """
        return prod(self.shape)
