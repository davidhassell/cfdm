from math import prod

from .abstract import PropertiesData, ProbabilityDistribution


class Uncertainty(PropertiesData):
    """An uncertainty construct of the CF data model.

    TODOU (copy from appendix I when merged)

    .. versionadded:: (cfdm) NEXTVERSION

    """
    __ProbabilityDistribution = ProbabilityDistribution
    
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
        elif coverage_interval is None:
            raise AttributeError("TODOU")
            
        return super().ndim

    @property
    def probability_distribution(self):
        """TODOU Return the coordinate conversion component.

        .. versionadded:: (cfdm) 1.7.0

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
        return self.get_probability_distribution()


        def get_probability_distribution(self):
        """TODOU Get the coordinate conversion component.

        .. versionadded:: (cfdm) 1.7.0

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
        out = self._get_component("probability_distribution", None)
        if out is None:
            out = self.__ProbabilityDistribution()
            self.set_probability_distribution(out, copy=False)

        return out

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
        elif coverage_interval is None:
            raise AttributeError("TODOU")

        return super().shape

    @property
    def size(self):
        """The number elements in the data.

        `size` is equal to the product of `shape`, that only includes
        the data dimension corresponding to a domain axis construct.

        .. versionadded:: (cfdm) 1.11.0.0

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
        elif coverage_interval is None:
            raise AttributeError("TODOU")

        return super().size
    
    def set_coordinate_conversion(self, coordinate_conversion, copy=True):
        """Set the coordinate conversion component.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `coordinate_conversion`, `del_coordinate_conversion`,
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
            probability_distribution = probability_distribution.copy()

        self._set_component(
            "probability_distribution", probability_distribution, copy=False
        )
