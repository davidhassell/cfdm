from .abstract import Parameters


class ErrorCorrelationModel(Parameters):
    """Mixin to collect named parameters andTODOU domain ancillaries.

    .. versionadded:: (cfdm) NEXTVERSION

    """

    def __init__(
        self,
        structure=None,
        comment=None,
        parameters=None,
        source=None,
        copy=True,
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
        super().__init__(parameters=parameters, source=source, copy=copy)

        if source:
            try:
                structure = source.get_structure(None)
            except AttributeError:
                structure = None

            try:
                comment = source.get_comment(None)
            except AttributeError:
                comment = None

        if structure is not None:
            self.set_structure(structure, copy=False)

        if comment is not None:
            self.set_comment(comment, copy=False)

    def del_comment(self, default=ValueError()):
        """Remove the comment.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `get_comment`, `has_comment`, `set_comment`

        :Parameters:

            default: optional
                Return the value of the *default* parameter if the
                comment has not been set.

                {{default Exception}}

        :Returns:

                The removed comment.

        **Examples**

        >>> c = {{package}}.{{class}}()
        >>> c.set_comment('area')
        >>> c.has_comment()
        True
        >>> c.get_comment()
        'area'
        >>> c.del_comment()
        'area'
        >>> c.has_comment()
        False
        >>> print(c.del_comment(None))
        None
        >>> print(c.get_comment(None))
        None

        """
        return self._del_component("comment", default=default)

    def del_structure(self, default=ValueError()):
        """Remove the structure.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `get_structure`, `has_structure`, `set_structure`

        :Parameters:

            default: optional
                Return the value of the *default* parameter if the
                structure has not been set.

                {{default Exception}}

        :Returns:

                The removed structure.

        **Examples**

        >>> c = {{package}}.{{class}}()
        >>> c.set_structure('area')
        >>> c.has_structure()
        True
        >>> c.get_structure()
        'area'
        >>> c.del_structure()
        'area'
        >>> c.has_structure()
        False
        >>> print(c.del_structure(None))
        None
        >>> print(c.get_structure(None))
        None

        """
        return self._del_component("structure", default=default)

    def get_comment(self, default=ValueError()):
        """Return a construct parameter.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `del_construct`, `constructs`, `set_construct`

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
        out = self._get_component("comment", None)
        if out is None:
            if default is None:
                return

            return self._default(
                default, f"{self.__class__.__name__!r} has no comment"
            )

        return out

    def get_structure(self, default=ValueError()):
        """Return a construct parameter.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `del_construct`, `constructs`, `set_construct`

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
        out = self._get_component("structure", None)
        if out is None:
            if default is None:
                return

            return self._default(
                default, f"{self.__class__.__name__!r} has no structure"
            )

        return out

    def has_comment(self):
        """Whether the comment has been set.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `del_comment`, `get_comment`, `set_comment`

        :Returns:

            `bool`
                True if the comment has been set, otherwise False.

        **Examples**

        >>> c = {{package}}.{{class}}()
        >>> c.set_comment('area')
        >>> c.has_comment()
        True
        >>> c.get_comment()
        'area'
        >>> c.del_comment()
        'area'
        >>> c.has_comment()
        False
        >>> print(c.del_comment(None))
        None
        >>> print(c.get_comment(None))
        None

        """
        return self._has_component("comment")

    def has_structure(self):
        """Whether the structure has been set.

        .. versionadded:: (cfdm) 1.7.0

        .. seealso:: `del_structure`, `get_structure`, `set_structure`

        :Returns:

            `bool`
                True if the structure has been set, otherwise False.

        **Examples**

        >>> c = {{package}}.{{class}}()
        >>> c.set_structure('area')
        >>> c.has_structure()
        True
        >>> c.get_structure()
        'area'
        >>> c.del_structure()
        'area'
        >>> c.has_structure()
        False
        >>> print(c.del_structure(None))
        None
        >>> print(c.get_structure(None))
        None

        """
        return self._has_component("structure")

    def set_comment(self, comment):
        """Set constructs.

        .. versionadded:: (cfdm) NEXTVERSION0

        .. seealso:: `del_comment`, `get_comment`, `has_comment`

        :Parameters:

            error_correlation_parameters: `dict`
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
        self._set_component("comment", comment, copy=False)

    def set_structure(self, structure):
        """Set constructs.

        .. versionadded:: (cfdm) NEXTVERSION0

        .. seealso:: `del_structure`, `get_structure`, `has_structure`

        :Parameters:

            error_correlation_parameters: `dict`
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
        self._set_component("structure", structure, copy=False)
