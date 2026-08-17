from math import prod

from .abstract import PropertiesData
from .probabilitydistribution import ProbabilityDistribution


class Uncertainty(PropertiesData):
    """An uncertainty construct of the CF data model.

    The uncertainty construct provides metadata that describe a
    component of the uncertainty in the field construct's data, and
    which are distributed over the same sampling domain as the field
    itself. It consists of the following:
    
    * An array that defines the coverage interval within which the
      field construct's true data values occur with a known
      probability. The array depends on zero or more of the domain
      axis constructs. If there is the addition of a trailing size-two
      dimension, then this stores the lower an upper limits of the
      interval. Otherwise the array values define the half-width of a
      symmetric interval. It is assumed that the data do not depend on
      axes of the domain which are not spanned by the array, along
      which the values are implicitly propagated.
    
    * Properties to describe the data (in the same sense as for the
      field construct). The properties must include a "coverage
      probability" property to indicate the probability that a true
      value of the field construct's data lies in the coverage
      interval defined by the array.
    
    * An optional definition of the probability distribution from
      which the coverage interval is derived. The probability
      distribution is defined by the values of named parameters. A
      parameter value can be a descriptive string (such as the
      distribution type "gaussian"), or an uncertainty ancillary
      construct (such as one containing spatially varying skewness
      data), or one or more uncertainty ancillary constructs (such as
      multiple uncertainty ancillary constructs containing
      error-correlation data for non-overlapping subsets of the domain
      axis constructs).
    
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

               *Example:*
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

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `data`, `has_data`, `shape`, `size`

        **Examples**

        >>> u.shape
        (1324,)
        >>> u.ndim
        1
        >>> u.size
        1324

        """
        if self.get_property("coverage_interval", None) == "offsets":
            try:
                return len(self.shape)
            except AttributeError:
                raise AttributeError(
                    f"{self.__class__.__name__} object has no attribute 'ndim'"
                )

        return super().ndim

    @property
    def probability_distribution(self):
        """TODOU Return the coordinate conversion component.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `del_probability_distribution`,
                     `get_probability_distribution`,
                     `set_probability_distribution`

        :Returns:

            `ProbabilityDistribution`
                The coordinate TODOUconversion.

        **Examples**

        >>> orog = {{package}}TODOU.DomainAncillary()
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
        if self.get_property("coverage_interval", None) == "offsets":
            data = self.get_data(None, _units=False, _fill_value=False)
            if data is not None:
                return data.shape[:-1]

            raise AttributeError(
                f"{self.__class__.__name__} object has no attribute 'shape'"
            )

        return super().shape

    @property
    def size(self):
        """The number elements in the data.
        TODOU
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
        if self.get_property("coverage_interval", None) == "offsets":
            try:
                return prod(self.shape[:-1])
            except AttributeError:
                raise AttributeError(
                    f"{self.__class__.__name__} object has no attribute 'size'"
                )

        return super().size

    def del_probability_distribution(self):
        """Remove the coordinate conversion component.
        TODOU

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `probability_distribution`,
                     `get_probability_distribution`,
                     `set_probability_distribution`

        :Returns:

            `ProbabilityDistribution`
                The removed TODOU coordinate conversion component.

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

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `probability_distribution`,
                     `del_probability_distribution`,
                     `set_probability_distribution`

        :Returns:

            `ProbabilityDistribution`
                TheTODOU coordinate conversion component.

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

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `probability_distribution`,
                     `del_probability_distribution`,
                     `get_probability_distribution`

        :Parameters:

            probability_distribution: `ProbabilityDistribution`
                The coordinate conversiTODOUon component to be inserted.

            {{copy: `bool`, optional}}

        :Returns:

            `None`

        **Examples**

        >>> r = {{package}}.TODOU{{class}}()
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
