from math import prod

from . import ErrorCorrelationModel
from .abstract import PropertiesData


class UncertaintyAncillary(PropertiesData):
    """An uncertainty ancillary construct of the CF data model.

    TODOU (adapt from from CF Appendix I)

    .. versionadded:: (cfdm) NEXTVERSION

    """

    def __new__(cls, *args, **kwargs):
        """Store component classes."""
        instance = super().__new__(cls)
        instance._ErrorCorrelationModel = ErrorCorrelationModel
        return instance

    def __init__(
        self,
        properties=None,
        data=None,
        distribution_parameter=None,
        error_correlation_model=None,
        source=None,
        copy=True,
        _use_data=True,
    ):
        """**Initialisation**

        :Parameters:

            parameters: `dict`, optional
               Set parameters. The dictionary keys are parameter
               names, with corresponding parameter values.

               Parameters may also be set after initialisation with
               the `set_parameters` and `set_parameter` methods.

               *Parameter example:*
                 ``parameters={'earth_radius': 6371007.}``

            constructs: `dict`, optional
               Set references to constructs. The dictionary keys are
               parameter names, with corresponding construct keys.

               Constructs may also be set after initialisation with
               the `set_constructs` and `set_construct` methods.

            {{init source: optional}}

            {{init copy: `bool`, optional}}

        """

        # A probability distribution, which defines a formula for
        # converting coordinate values taken from the dimension or
        # auxiliary coordinate constructs to a different coordinate
        # system. A term of the conversion formula can be a scalar or
        # vector parameter which does not depend on any domain axis
        # constructs, may have units (such as a reference pressure
        # value), or may be a descriptive string (such as the
        # projection name "mercator"), or it can be a domain ancillary
        # construct (such as one containing spatially varying
        # orography data).
        super().__init__(
            properties=properties,
            data=data,
            source=source,
            copy=copy,
            _use_data=_use_data,
        )

        if source:
            try:
                distribution_parameter = source.get_distribution_parameter(
                    None
                )
            except AttributeError:
                distribution_parameter = None

            try:
                error_correlation_model = source.get_error_correlation_model()
            except AttributeError:
                error_correlation_model = None

        if distribution_parameter is not None:
            self.set_distribution_parameter(distribution_parameter)

        if error_correlation_model is not None:
            self.set_error_correlation_model(
                error_correlation_model, copy=copy
            )

    @property
    def error_correlation_model(self):
        """Return the coordinate conversion component.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `datum`, `get_coordinate_conversion`

        :Returns:

            `CoordinateConversion`
                The coordinate conversion.

        **Examples**

        >>> orog = {{package}}.DomainAncillary()
        >>> c = {{package}}.CoordinateConversion(
        ...     parameters={
        ...         'standard_name': 'atmosphere_hybrid_height_coordinate',
        ...     },
        ...     domain_ancillaries={'orog': orog}
        ... )
        >>> r = {{package}}.{{class}}(coordinate_conversion=c)
        >>> r.coordinate_conversion
        <{{repr}}CoordinateConversion: Parameters: standard_name; Ancillaries: orog>

        """
        return self.get_error_correlation_model()

    @property
    def construct_type(self):
        """Return a description of the construct type.TODOU

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
        """The number of data dimensions.TODOU

        Only the data dimensions that correspond to a domain axis
        construct are included.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `data`, `has_data`, `shape`, `size`

        **Examples**

        >>> d.shape
        (1324,)
        >>> d.ndim
        1
        >>> f.size
        1324

        """
        return len(self.shape)

    @property
    def shape(self):
        """A tuple of the data array's dimension sizes.TODOU

        Only the data dimension that corresponds to a domain axis
        construct is included.

        .. versionadded:: (cfdm) NEXTVERSION

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
        if self.get_distribution_parameter() == "error_correlation":
            shape = shape[: len(shape) // 2]

        return shape

    @property
    def size(self):
        """The number elements in the data. TODOU

        `size` is equal to the product of `shape`, that only includes
        the data dimension corresponding to a domain axis construct.

        .. versionadded:: (cfdm) NEXTVERSION

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

    def get_distribution_parameter(self, default=ValueError()):
        """Get a parameter value.

        .. versionadded:: (cfdm) 1.7.0

        :Parameters:

            parameter: `str`
                The name of the parameter.

            default: optional
                Return the value of the *default* parameter if the
                parameter has not been set.

                {{default Exception}}

        :Returns:

                The value of the parameter.

        **Examples**

        >>> f = {{package}}.{{class}}()
        >>> f.set_parameter('earth_radius', 6371007)
        >>> f.has_parameter('earth_radius')
        True
        >>> f.get_parameter('earth_radius')
        6371007
        >>> f.del_parameter('earth_radius')
        6371007
        >>> f.has_parameter('earth_radius')
        False
        >>> print(f.del_parameter('earth_radius', None))
        None
        >>> print(f.get_parameter('earth_radius', None))
        None

        """
        out = self._get_component("distribution_parameter", None)
        if out is None:
            if default is None:
                return

            return self._default(
                default,
                f"{self.__class__.__name__!r} has no distribution parameter",
            )

        return out

    def get_error_correlation_model(self):
        """Get the coordinate conversion component.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `coordinate_conversion`, `del_coordinate_conversion`,
                     `set_coordinate_conversion`

        :Returns:

            `CoordinateConversion`
                The coordinate conversion component.

        **Examples**

        >>> r = {{package}}.{{class}}()
        >>> orog = {{package}}.DomainAncillary()
        >>> c = {{package}}.CoordinateConversion(
        ...     parameters={
        ...         'standard_name': 'atmosphere_hybrid_height_coordinate',
        ...     },
        ...     domain_ancillaries={'orog': orog}
        ... )
        >>> r.set_coordinate_conversion(c)
        >>> r.get_coordinate_conversion()
        <{{repr}}CoordinateConversion: Parameters: standard_name; Ancillaries: orog>
        >>> r.del_coordinate_conversion()
        <{{repr}}CoordinateConversion: Parameters: standard_name; Ancillaries: orog>
        >>> r.get_coordinate_conversion()
        <{{repr}}CoordinateConversion: Parameters: ; Ancillaries: >

        """
        out = self._get_component("error_correlation_model", None)
        if out is None:
            out = self._ErrorCorrelationModel()
            self.set_error_correlation_model(out, copy=False)

        return out

    def set_distribution_parameter(self, distribution_parameter):
        """Set the measure.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `del_measure`, `get_measure`, `has_measure`

        :Parameters:

            measure: `str`
                The value for the measure.

        :Returns:

             `None`

        **Examples**

        >>> c = {{package}}.{{class}}()
        >>> c.set_measure('area')
        >>> c.has_measure()
        True
        >>> c.get_measure()
        'area'
        >>> c.del_measure()
        'area'
        >>> c.has_measure()
        False
        >>> print(c.del_measure(None))
        None
        >>> print(c.get_measure(None))
        None

        """
        self._set_component(
            "distribution_parameter", distribution_parameter, copy=False
        )

    def set_error_correlation_model(self, error_correlation_model, copy=True):
        """Set thTODOU e coordinate conversion component.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `coordinate_conversion`,
                     `del_coordinate_conversion`,
                     `get_coordinate_conversion`

        :Parameters:

            coordinate_conversion: `CoordinateConversion`
                The coordinate conversion component to be inserted.

            {{copy: `bool`, optional}}

        :Returns:

            `None`

        **Examples**

        >>> r = {{package}}.{{class}}()
        >>> orog = {{package}}.DomainAncillary()
        >>> c = {{package}}.CoordinateConversion(
        ...     parameters={
        ...         'standard_name': 'atmosphere_hybrid_height_coordinate',
        ...     },
        ...     domain_ancillaries={'orog': orog}
        ... )
        >>> r.set_coordinate_conversion(c)
        >>> r.get_coordinate_conversion()
        <{{repr}}CoordinateConversion: Parameters: standard_name; Ancillaries: orog>
        >>> r.del_coordinate_conversion()
        <{{repr}}CoordinateConversion: Parameters: standard_name; Ancillaries: orog>
        >>> r.get_coordinate_conversion()
        <{{repr}}CoordinateConversion: Parameters: ; Ancillaries: >

        """
        if copy:
            error_correlation_model = error_correlation_model.copy()

        self._set_component(
            "error_correlation_model", error_correlation_model, copy=False
        )
