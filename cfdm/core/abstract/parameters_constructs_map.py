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
                distribution = source.get_distribution()
            except AttributeError:
                distribution  = None

            try:                
                distribution_parameters = source.distribution_parameters()
            except AttributeError:
                distribution_parameters  = None

            try:                
                error_correlations == source.error_correlations()
            except AttributeError:
                error_correlations  = None

        if distribution_parameters is None:
           distribution_parameters = {}

        if error_correlations is None:
            error_correlations = {}
            
        if distribution is not None:
            self.set_distribution(distribution, copy=False)

        self.set_distribution_parameters(distribution_parameters, copy=copy)
        self.set_error_correlations(error_correlations, copy=copy)

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
        out = self._del_component("distribution_parameters", None)
        if out is None:
            return {}
        
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
        dp = self._get_component("distribution_parameters", None)
        try:
            return dp.pop(distribution_parameter)
        except (KeyError, AttributeError):
            if default is None:
                return

            return self._default(
                default,
                f"{self.__class__.__name__!r} has no "
                f"{distribution_parameter!r} distribution parameter",
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
        out = self._get_component("distribution_parameters", None)
        if out is None:
            return {}

        return out.copy()

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
        dp =  self._get_component("distribution_parameters", None)
        try:
            return dp[parameter]
        except (KeyError, TypeError):
            if default is None:
                return

            return self._default(
                default,
                f"{self.__class__.__name__!r} has no "
                f"{distribution_parameter!r} distribution parameter",
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
        dp = self._get_component("distribution_parameters", None)
        if dp is None:
            dp = distribution_parameters.copy()
            self._set_component("distribution_parameters", dp, copy=False)
        else:
            dp.update(distribution_parameters)

                            
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
        dp = self._get_component("distribution_parameters", None)
        if dp is None:
            dp = {parameter: value}
            self._get_component("distribution_parameters", dp, copy=False)
        else:
            dp[parameter] = value            

    def clear_error_correlations(self):
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
        out = self._del_component("error_correlations", None)
        if out is None:
            return set()
        
        return out.copy()

    def del_error_correlation(self, error_correlation, default=ValueError()):
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
        ec = self._get_component("error_correlations", None)
        try:
            return ec.remove(error_correlation)
        except (KeyError, AttributeError):
            if default is None:
                return

            return self._default(
                default,
                f"{self.__class__.__name__!r} has no {error_correlation!r} "
                "error correlation",
            )

    def error_correlations(self):
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
        ec = self._get_component("error_correlations", None)
        if ec is None:
            return set()

        return ec.copy()

    def set_error_correlations(self, error_correlations, copy=True):
        """Set constructs.

        .. versionadded:: (cfdm) NEXTVERSION0

        .. seealso:: `clear_constructs`, `constructs`,
                     `set_construct`

        :Parameters:

            error_correlations: sequence if `str`
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
        ec = self._get_component("error_correlations", None)
        if ec is None:
            ec = set(error_correlations)
            self._set_component("error_correlations", ec, copy=False)
        else:
            ec.update(error_correlations)
            
                            
    def set_error_correlation(self, error_correlation):
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
        ec = self._get_component("error_correlations", None)
        if ec is None:
            ec = set((error_correlation,))
            ec = self._set_component("error_correlations", ec, copy=False)
        else:
            ec.add(error_correlation)
            
    def del_distribution(self, default=ValueError()):
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
        out = self._del_component("distribution", None)
        if out is None:
            if default is None:
                return
            
            return self._default(
                default,
                f"{self.__class__.__name__!r} has distribution",
            )
        
        return out
    
    def get_distribution(self, default=ValueError()):
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
        out =  self._get_component("distribution", None)
        if out is None:
            if default is None:
                return

            return self._default(
                default,
                f"{self.__class__.__name__!r} has no distribution"
            )

        return out
    
    def set_distribution(self, distribution):
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
        self._set_component("distribution", distribution, copy=False)
                            
