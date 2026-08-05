from math import prod

from .abstract import PropertiesData
from .probabilitydistribution import ProbabilityDistribution


class Uncertainty(PropertiesData):
    """An uncertainty construct of the CF data model.

    TODOU (copy from appendix I when merged)

    .. versionadded:: (cfdm) NEXTVERSION

    """

    def __new__(cls, *args, **kwargs):
        """Store component classes."""
        instance = super().__new__(cls)
        instance._ProbabilityDistribution = ProbabilityDistribution
        return instance

    def __init__(
        self,
        properties=None,
        data=None,
        probability_distribution=None,
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

        super().__init__(
            properties=properties,
            data=data,
            source=source,
            copy=copy,
            _use_data=_use_data,
        )

        if source:
            try:
                probability_distribution = (
                    source.get_probability_distribution()
                )
            except AttributeError:
                probability_distribution = None

        if probability_distribution is not None:
            self.set_probability_distribution(
                probability_distribution, copy=copy
            )

    @property
    def construct_type(self):
        """Return a description of the construct type.

        .. versionadded:: (cfdm) NEXTVERSION

        :Returns:

            `str`
                The construct type.

        **Examples**
        TODOU
        >>> f = {{package}}.{{class}}()
        >>> f.construct_type
        'uncertainty'

        """
        return "uncertainty"

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
        coverage_interval = self.get_property("coverage_interval", None)
        if coverage_interval == "offsets":
            try:
                return len(self.shape) - 1
            except AttributeError:
                raise AttributeError(
                    f"{self.__class__.__name__} object has no attribute 'ndim'"
                )

        elif coverage_interval in ("standard_deviation", "half_width"):
            return super().ndim

        raise AttributeError("TODOU")

    @property
    def probability_distribution(self):
        """TODOU Return the coordinate conversion component.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `datum`, `get_probability_distribution`

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
        >>> r = {{package}}.{{class}}(probability_distribution=c)
        >>> r.probability_distribution
        <{{repr}}CoordinateConversion: Parameters: standard_name; Ancillaries: orog>

        """
        return self.get_probability_distribution()

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
        coverage_interval = self.get_property("coverage_interval", None)
        if coverage_interval == "offsets":
            data = self.get_data(None, _units=False, _fill_value=False)
            if data is not None:
                return data.shape[:1]

            raise AttributeError(
                f"{self.__class__.__name__} object has no attribute 'shape'"
            )

        elif coverage_interval in ("standard_deviation", "half_width"):
            return super().shape

        raise AttributeError("TODOU")

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
        coverage_interval = self.get_property("coverage_interval", None)
        if coverage_interval == "offsets":
            try:
                return prod(self.shape[:-1])
            except AttributeError:
                raise AttributeError(
                    f"{self.__class__.__name__} object has no attribute 'size'"
                )

        elif coverage_interval in ("standard_deviation", "half_width"):
            return super().size

        raise AttributeError("TODOU")

    def del_probability_distribution(self):
        """Remove the coordinate conversion component.
        TODOU

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `probability_distribution`,
                     `get_probability_distribution`,
                     `set_probability_distribution`

        :Returns:

            `CoordinateConversion`
                The removed coordinate conversion component.

        **Examples**

        >>> r = {{package}}.{{class}}()
        >>> orog = {{package}}.DomainAncillary()
        >>> c = {{package}}.CoordinateConversion(
        ...     parameters={
        ...         'standard_name': 'atmosphere_hybrid_height_coordinate',
        ...     },
        ...     domain_ancillaries={'orog': orog}
        ... )
        >>> r.set_probability_distribution(c)
        >>> r.get_probability_distribution()
        <{{repr}}CoordinateConversion: Parameters: standard_name; Ancillaries: orog>
        >>> r.del_probability_distribution()
        <{{repr}}CoordinateConversion: Parameters: standard_name; Ancillaries: orog>
        >>> r.get_probability_distribution()
        <{{repr}}CoordinateConversion: Parameters: ; Ancillaries: >

        """
        out = self.get_probability_distribution()
        self._del_component("probability_distribution", None)
        return out

    def get_probability_distribution(self):
        """TODOU Get the coordinate conversion component.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `probability_distribution`,
                     `del_probability_distribution`,
                     `set_probability_distribution`

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
        >>> r.set_probability_distribution(c)
        >>> r.get_probability_distribution()
        <{{repr}}CoordinateConversion: Parameters: standard_name; Ancillaries: orog>
        >>> r.del_probability_distribution()
        <{{repr}}CoordinateConversion: Parameters: standard_name; Ancillaries: orog>
        >>> r.get_probability_distribution()
        <{{repr}}CoordinateConversion: Parameters: ; Ancillaries: >

        """
        out = self._get_component("probability_distribution", None)
        if out is None:
            out = self._ProbabilityDistribution()
            self.set_probability_distribution(out, copy=False)

        return out

    def set_probability_distribution(
        self, probability_distribution, copy=True
    ):
        """Set the coordinate conversion component.
        TODOU

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `probability_distribution`,
                     `del_probability_distribution`,
                     `get_probability_distribution`

        :Parameters:

            probability_distribution: `CoordinateConversion`
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
        >>> r.set_probability_distribution(c)
        >>> r.get_probability_distribution()
        <{{repr}}CoordinateConversion: Parameters: standard_name; Ancillaries: orog>
        >>> r.del_probability_distribution()
        <{{repr}}CoordinateConversion: Parameters: standard_name; Ancillaries: orog>
        >>> r.get_probability_distribution()
        <{{repr}}CoordinateConversion: Parameters: ; Ancillaries: >

        """
        if copy:
            probability_distribution = probability_distribution.copy()

        self._set_component(
            "probability_distribution", probability_distribution, copy=False
        )
