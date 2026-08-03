from .parameters import Parameters


class ProbabilityDistribution(Parameters):
    """Mixin to collect named parameters andTODOU domain ancillaries.

    .. versionadded:: (cfdm) NEXTVERSION

    """

    def __init__(
            self, distribution=None, parameters=None,
            distribution_parameters=None, error_correlations=None,
            source=None, copy=True
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

#        A probability distribution, which defines a formula for converting coordinate values taken from the dimension or auxiliary coordinate constructs to a different coordinate system. A term of the conversion formula can be a scalar or vector parameter which does not depend on any domain axis constructs, may have units (such as a reference pressure value), or may be a descriptive string (such as the projection name "mercator"), or it can be a domain ancillary construct (such as one containing spatially varying orography data).
        super().__init__(parameters=parameters, source=source, copy=copy)
        if source:
            try:                
                distribution_parameters = source.distribution_parameters()
            except AttributeError:
                distribution_parameters  = None

            try:                
                error_correlations == source.error_correlations()
            except AttributeError:
                error_correlationss  = None

        if  distribution_parameters is None:
           distribution_parameters = {}

        if error_correlations is None:
            error_correlations = {}
            
        self.set_distribution_parameters(distribution_parameters, copy=False)
        self.set_error_correlations(error_correlations, copy=False)

    def clear_distribution_parameters(self):
        """Remove all constructs.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `del_constructs`, `constructs`, `set_constructs`

        :Returns:

            `dict`
                The constructs that have been removed.

        **Examples**

        >>> f = {{package}}.{{class}}()
        >>> f.domain_ancillaries() TODOU
        {}
        >>> d = {'a': 'domainancillary0',
        ...      'b': 'domainancillary1',
        ...      'orog': 'domainancillary2'}
        >>> f.set_domain_ancillaries(d)
        >>> f.domain_ancillaries()
        {'a': 'domainancillary0',
         'b': 'domainancillary1',
         'orog': 'domainancillary2'}

        >>> old = f.clear_domain_ancillaries()
        >>> f.domain_ancillaries()
        {}
        >>> old
        {'a': 'domainancillary0',
         'b': 'domainancillary1',
         'orog': 'domainancillary2'}

        """
        out = self._del_component("distribution_parameters", {})
        return out.copy()

    def del_distribution_parameter(self, distribution_parameter, default=ValueError()):
        """Delete a construct.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `construct`, `get_construct`, `set_construct`

        :Parameters:

            construct: `str`
                The name of the construct to be deleted.

            default: optional
                Return the value of the *default* parameter if the
                constructs parameter has not been set.

                {{default Exception}}

        :Returns:

            `str`
                The removed construct key.

        **Examples**

        >>> TODOU c = {{package}}.{{class}}()
        >>> c.set_construct('orog', 'domainancillary2')
        >>> c.has_construct('orog')
        True
        >>> c.get_construct('orog')
        'domainancillary2'
        >>> c.del_construct('orog')
        'domainancillary2'
        >>> c.has_construct('orog')
        False
        >>> print(c.del_construct('orog', None))
        None
        >>> print(c.get_construct('orog', None))
        None

        """
        try:
            return self._get_component("distribution_parameters").pop(
                distribution_parameter
            )
        except KeyError:
            if default is None:
                return

            return self._default(
                default,
                f"{self.__class__.__name__!r} has no {distribution_parameter!r} distribution_parameter",
            )

    def distribution_parameters(self):
        """Return all constructs.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `clear_constructs`, `get_construct`,
                     `has_construct` `set_constructs`

        :Returns:

            `dict`
                The constructs.

        **Examples**

        >>> f = {{package}}.{{class}}()
        >>> f.constructs()
        {}
        >>> d = {'a': 'domainancillary0',
        ...      'b': 'domainancillary1',
        ...      'orog': 'domainancillary2'}
        >>> f.set_constructs(d)
        >>> f.constructs()
        {'a': 'domainancillary0',
         'b': 'domainancillary1',
         'orog': 'domainancillary2'}

        >>> old = f.clear_constructs()
        >>> f.constructs()
        {}
        >>> old
        {'a': 'domainancillary0',
         'b': 'domainancillary1',
         'orog': 'domainancillary2'}

        """
        return self._get_component("distribution_parameter").copy()

    def get_distribution_parameter(self, parameter, default=ValueError()):
        """Return a construct parameter.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `del_construct`, `constructs`,
                     `set_construct`

        :Parameters:

            parameter: `str`
                The name of the parameter.

            default: optional
                Return the value of the *default* parameter if the
                construct has not been set.

                {{default Exception}}

        :Returns:

                The construct key.

        **Examples**

        >>> c = {{package}}.{{class}}()
        >>> c.set_construct('orog', 'domainancillary2')
        >>> c.has_construct('orog')
        True
        >>> c.get_construct('orog')
        'domainancillary2'
        >>> c.del_construct('orog')
        'domainancillary2'
        >>> c.has_construct('orog')
        False
        >>> print(c.del_construct('orog', None))
        None
        >>> print(c.get_construct('orog', None))
        None

        """
        try:
            return self._get_component("distribution_parameters")[construct]
        except KeyError:
            if default is None:
                return

            return self._default(
                default,
                f"{self.__class__.__name__!r} has no {distribution_parameter!r} distribution_parameter",
            )

    def set_distribution_parameters(self, distribution_parameters, copy=True):
        """Set constructs.

        .. versionadded:: (cfdm) NEXTVERSION0

        .. seealso:: `clear_constructs`, `constructs`,
                     `set_construct`

        :Parameters:

            distribution_parameters: `dict`
                Store the constructs from the dictionary supplied.

                *Parameter example:*
                  ``constructs={'earth_radius': 6371007}``

            copy: `bool`, optional
                If False then any parameter values provided by the
                *constructs* parameter are not copied before
                insertion. By default they are deep copied.

        :Returns:

            `None`

        **Examples**

        >>> f = {{package}}.{{class}}()
        >>> f.constructs()
        {}
        >>> d = {'a': 'domainancillary0',
        ...      'b': 'domainancillary1',
        ...      'orog': 'domainancillary2'}
        >>> f.set_constructs(d)
        >>> f.constructs()
        {'a': 'domainancillary0',
         'b': 'domainancillary1',
         'orog': 'domainancillary2'}

        >>> old = f.clear_constructs()
        >>> f.constructs()
        {}
        >>> old
        {'a': 'domainancillary0',
         'b': 'domainancillary1',
         'orog': 'domainancillary2'}

        """
        dp = self._get_component("distribution_parameter")
        dp.update(distribution_parameters)
        self._set_component("distribution_parameter", copy=False)
                            
    def set_distribution_parameter(self, parameter, value, copy=True):
        """Set a construct-valued parameter.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `del_construct`, `constructs`, `get_construct`

        :Parameters:

            parameter: `str`
                The name of the term to be set.

            value:
                The value for the parameter.

            copy: `bool`, optional
                If True then set a deep copy of *value*.

        :Returns:

            `None`

        **Examples**

        >>> c = {{package}}.{{class}}()
        >>> c.set_construct('orog', 'domainancillary2')
        >>> c.has_construct('orog')
        True
        >>> c.get_construct('orog')
        'domainancillary2'
        >>> c.del_construct('orog')
        'domainancillary2'
        >>> c.has_construct('orog')
        False
        >>> print(c.del_construct('orog', None))
        None
        >>> print(c.get_construct('orog', None))
        None

        """
        self._get_component("distribution_parameter")[parameter] = value
