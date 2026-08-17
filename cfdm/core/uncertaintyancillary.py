from math import prod

from . import UncertaintyAncillaryParameterisation
from .abstract import PropertiesData


class UncertaintyAncillary(PropertiesData):
    """An uncertainty ancillary construct of the CF data model.

    An uncertainty ancillary construct provides metadata for
    describing probability distribution of the field contruct values
    (i.e. the probability distribution of the true unknowable values
    of the measurands), which depend on a subset of zero or more of
    the domain axis contructs. For instance, an uncertainty ancillary
    construct could provide the skewness of the probability
    distribution, error-correlation coefficients, or the value of a
    term in an error-correlation's parametric form. An uncertainty
    ancillary construct consists of the following:

    * An optional data array of values that depends on the subset of
      zero or more domain axis constructs, and describes an aspect the
      probability distribution at the locations indexed by the domain
      axis constructs. It is assumed that the data do not depend on
      axes of the domain which are not spanned by the array, along
      which the values are implicitly propagated. When the joint
      probability distribution of the measurands is being described,
      the array may also span extra trailing dimensions, one for each
      domain construct spanned by the array, with the same size and in
      the same order.

    * Properties to describe the data array (in the same sense as for
      the field construct). The properties may include a "comment"
      property that provides a general description of the numerical
      structure of the data array, even when no array has been
      provided.

    * When the data array is omitted, the uncertainty ancillary
      construct still depends on a subset of zero or more of the
      domain axis contructs, and there may be a parameterization
      formula which describes how the missing data array can be
      created. A term of the parameterization formula can be a
      descriptive string (such as the error-correlation structural
      type "triangular"), or can may be another uncertainty ancillary
      construct (such as one which contains a configuration parameter
      for an error-correlation structural type).

    .. versionadded:: (cfdm) NEXTVERSION

    """

    def __new__(cls, *args, **kwargs):
        """Store component classes."""
        instance = super().__new__(cls)
        instance._UncertaintyAncillaryParameterisation = (
            UncertaintyAncillaryParameterisation
        )
        return instance

    def __init__(
        self,
        properties=None,
        data=None,
        parameterisation=None,
        trailing_dimensions=None,
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

            data: TODOU

            parameterisation=None,  TODOU
            trailing_dimensions=None,  TODOU

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
                parameterisation = source.get_parameterisation()
            except AttributeError:
                parameterisation = None

            try:
                trailing_dimensions = source.has_trailing_dimensions()
            except AttributeError:
                trailing_dimensions = None

        if parameterisation is not None:
            self.set_parameterisation(parameterisation, copy=copy)

        if trailing_dimensions is not None:
            self.set_trailing_dimensions(trailing_dimensions)

    @property
    def parameterisation(self):
        """Return the coordinate TODOUconversion component.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `get_TODOUcoordinate_conversion`

        :Returns:

            `CoordinateConversionTODOU`
                The coordinate TODOUconversion.

        **Examples**

        >>> orog = TODOU{{package}}.DomainAncillary()
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
        return self.get_parameterisation()

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
        if self.has_trailing_dimensions():
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

    def get_parameterisation(self):
        """Get theTODOU coordinate conversion component.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: TODOU`coordinate_conversion`, `del_coordinate_conversion`,
                     `set_coordinate_conversion`

        :Returns:

            `UncertaintyAncillaryParameterisation`
                The cooTODOUrdinate conversion component.

        **Examples**

        >>> TODOU r = {{package}}.{{class}}()
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
        out = self._get_component("parameterisation", None)
        if out is None:
            out = self._UncertaintyAncillaryParameterisation()
            self.set_parameterisation(out, copy=False)

        return out

    def has_trailing_dimensions(self):
        """TODOU.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `set_trailing_dimensions`

        :Returns:

            `bool`
                TODOU

        """
        out = self._get_component("trailing_dimensions", None)
        if out is None:
            raise AttributeError(
                f"{self.__class__.__name__} must specify whether or not "
                "it has trailing dimensions"
            )

        return out

    def set_parameterisation(self, parameterisation, copy=True):
        """Set thTODOU e coordinate conversion component.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `coordinate_conversion`,
                     `del_coordinate_conversion`,
                     `get_coordinate_conversion`

        :Parameters:

            parameterisation: `UncertaintyAncillaryParameterisation`
                The coordinate converTODOUsion component to be inserted.

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
        >>> r.set_coordinate_conversion(c)
        >>> r.get_coordinate_conversion()
        <{{repr}}CoordinateConversion: Parameters: standard_name; Ancillaries: orog>
        >>> r.del_coordinate_conversion()
        <{{repr}}CoordinateConversion: Parameters: standard_name; Ancillaries: orog>
        >>> r.get_coordinate_conversion()
        <{{repr}}CoordinateConversion: Parameters: ; Ancillaries: >

        """
        if copy:
            parameterisation = parameterisation.copy()

        self._set_component("parameterisation", parameterisation, copy=False)

    def set_trailing_dimensions(self, trailing_dimensions):
        """TODOU.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `has_trailing_dimensions`

        :Parameters:

            trailing_dimensions: `bool`
                TODOU

        :Returns:

            `None`

        """
        self._set_component(
            "trailing_dimensions", bool(trailing_dimensions), copy=False
        )
