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

    def del_parameter(self, default=ValueError()):
        """Remove the parameter.

        {{cell parameter type}}

        .. versionadded:: (cfdm) 1.11.0.0

        .. seealso:: `get_parameter`, `has_parameter`,
                     `set_parameter`

        :Parameters:

            default: optional
                Return the value of the *default* parameter if the
                parameter has not been set.

                {{default Exception}}

        :Returns:

                The removed parameter.

        **Examples**

        >>> d = {{package}}.{{class}}()
        >>> d.has_parameter()
        False
        >>> d.set_parameter('face')
        >>> d.has_parameter()
        True
        >>> d.get_parameter()
        'face'
        >>> d.del_parameter()
        'face'
        >>> d.get_parameter()
        Traceback (most recent call last):
            ...
        ValueError: {{class}} has no 'parameter' component
        >>> print(d.get_parameter(None))
        None

        """
        return self._del_component("parameter", default=default)

    def has_parameter(self):
        """Whether the parameter type has been set.

        {{cell parameter type}}

        .. versionadded:: (cfdm) 1.11.0.0

        .. seealso:: `del_parameter`, `get_parameter`,
                     `set_parameter`

        :Returns:

             `bool`
                `True` if the parameter has been set, otherwise
                `False`.

        **Examples**

        >>> d = {{package}}.{{class}}()
        >>> d.has_parameter()
        False
        >>> d.set_parameter('face')
        >>> d.has_parameter()
        True
        >>> d.get_parameter()
        'face'
        >>> d.del_parameter()
        'face'
        >>> d.get_parameter()
        Traceback (most recent call last):
            ...
        ValueError: {{class}} has no 'parameter' component
        >>> print(d.get_parameter(None))
        None

        """
        return self._has_component("parameter")

    def get_parameter(self, default=ValueError()):
        """Return the parameter type.

        {{cell parameter type}}

        See `set_parameter` for the parameter type definitions.

        .. versionadded:: (cfdm) 1.11.0.0

        .. seealso:: `del_parameter`, `has_parameter`,
                     `set_parameter`

        :Parameters:

            default: optional
                Return the value of the *default* parameter if the
                parameter has not been set.

                {{default Exception}}

        :Returns:

                The value of the parameter.

        **Examples**

        >>> d = {{package}}.{{class}}()
        >>> d.has_parameter()
        False
        >>> d.set_parameter('face')
        >>> d.has_parameter()
        True
        >>> d.get_parameter()
        'face'
        >>> d.del_parameter()
        'face'
        >>> d.get_parameter()
        Traceback (most recent call last):
            ...
        ValueError: {{class}} has no 'parameter' component
        >>> print(d.get_parameter(None))
        None

        """
        return self._get_component("parameter", default=default)

    def set_parameter(self, parameter):
        """Set the parameter type.

        {{cell parameter type}}

        .. versionadded:: (cfdm) 1.11.0.0

        .. seealso:: `del_parameter`, `get_parameter`,
                     `has_parameter`

        :Parameters:

            parameter: `str`
                The value for the parameter.

        :Returns:

             `None`

        **Examples**

        >>> d = {{package}}.{{class}}()
        >>> d.has_parameter()
        False
        >>> d.set_parameter('face')
        >>> d.has_parameter()
        True
        >>> d.get_parameter()
        'face'
        >>> d.del_parameter()
        'face'
        >>> d.get_parameter()
        Traceback (most recent call last):
            ...
        ValueError: {{class}} has no 'parameter' component
        >>> print(d.get_parameter(None))
        None

        """
        self._set_component("parameter", parameter, copy=False)
