from .parameters import Parameters


class ParametersAncillaries(Parameters):
    """Mixin to collect named parameters and ancillary constructs.

    Ancillary constructs can be domain or uncertainty ancillary
    constructs.

    .. versionadded:: (cfdm) NEXTVERSION

    """

    def __init__(
        self,
        parameters=None,
        ancillaries=None,
        multiple_ancillaries=False,
        source=None,
        copy=True,
    ):
        """**Initialisation**

        :Parameters:

            parameters: `dict`, optional
               Set parameters. The dictionary keys are term names,
               with corresponding parameter values.

               Parameters may also be set after initialisation with
               the `set_parameters` and `set_parameter` methods.

               *Example:*
                 ``parameters={'earth_radius': 6371007.}``

            ancillaries: `dict`, optional
               Set references to ancillary constructs. The dictionary
               keys are term names, with corresponding ancillary
               construct keys.

               Ancillaries may also be set after initialisation with
               the `set_ancillaries` and `set_ancillary` methods.

               *Example:*
                 ``ancillaries={'orog': 'domainancillary2'}``

            multiple_ancillaries: `bool`, optional

                TODOU


            {{init source: optional}}

            {{init copy: `bool`, optional}}

        """
        super().__init__(parameters=parameters, source=source, copy=copy)

        self._set_component("ancillaries", {}, copy=False)

        if source:
            try:
                ancillaries = source.ancillaries()
            except AttributeError:
                ancillaries = None

            try:
                multiple_ancillaries = source.get_multiple_ancillaries()
            except AttributeError:
                multiple_ancillaries = False

        if ancillaries is None:
            ancillaries = {}

        self.set_multiple_ancillaries(multiple_ancillaries)
        self.set_ancillaries(ancillaries)

    def clear_ancillaries(self):
        """Remove all ancillaries.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `del_ancillary`, `ancillaries`, `set_ancillaries`

        :Returns:

            `dict`
                The ancillaries that have been removed.

        """
        out = self._get_component("ancillaries")
        self._set_component("ancillaries", {})
        return out.copy()

    def del_ancillary(self, term, default=ValueError()):
        """Delete an ancillary construct-valued term.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `ancillaries`, `get_ancillary`, `set_ancillary`

        :Parameters:

            term: `str`
                The name of the ancillary to be deleted.

                *Example:*
                   ``ancillary='orog'``

            default: optional
                Return the value of the *default* parameter if the
                ancillary term has not been set.

                {{default Exception}}

        :Returns:

            `str`
                The removed ancillary key.

        """
        try:
            return self._get_component("ancillaries").pop(term)
        except KeyError:
            if default is None:
                return

            s = "s" if self.get_multiple_ancillaries() else ""

            return self._default(
                default,
                f"{self.__class__.__name__!r} has no {term!r} "
                f"ancillary construct{s}",
            )

    def ancillaries(self):
        """Return all ancillaries.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `clear_ancillaries`, `get_ancillary`,
                     `has_ancillary` `set_ancillaries`

        :Returns:

            `dict`
                The ancillaries.

        """
        return self._get_component("ancillaries").copy()

    def get_ancillary(self, term, default=ValueError()):
        """Return an ancillary construct-valued term.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `del_ancillary`, `ancillaries`, `set_ancillary`

        :Parameters:

            term: `str`
                The name of the term.

            default: optional
                Return the value of the *default* parameter if the
                ancillary term has not been set.

                {{default Exception}}

        :Returns:

                The ancillary construct key.

        """
        try:
            return self._get_component("ancillaries")[term]
        except KeyError:
            if default is None:
                return

            s = "s" if self.get_multiple_ancillaries() else ""

            return self._default(
                default,
                f"{self.__class__.__name__!r} has no {term!r} "
                f"ancillary construct{s}",
            )

    def get_multiple_ancillaries(self):
        """TODOU.

        .. versionadded:: (cfdm) NEXTVERSION

        :Returns:

            `bool`
                TODOU

        **Examples**

        >>> TODOU

        """
        return self._get_component("multiple_ancillaries")

    def has_ancillary(self, term):
        """Whether an ancillary construct-valued term has been set.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `del_ancillary`, `ancillaries`, `has_ancillary`,
                     `set_ancillary`

        :Parameters:

            term: `str`
                The name of the term.

        :Returns:

                The ancillary construct key.

        """
        return term in self._get_component("ancillaries")

    def set_ancillaries(self, ancillaries):
        """Set ancillary construct-valued terms.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `clear_ancillaries`, `ancillaries`,
                     `set_ancillary`

        :Parameters:

            ancillaries: `dict`
                Store the ancillaries from the dictionary supplied.

        :Returns:

            `None`

        """
        if self.get_multiple_ancillaries():
            ancillaries = {
                key: (
                    tuple([value]) if isinstance(value, str) else tuple(value)
                )
                for key, value in ancillaries.items()
            }

        self._get_component("ancillaries").update(ancillaries)

    def set_ancillary(self, term, ancillary):
        """Set an ancillary-valued parameter.

        .. versionadded:: (cfdm) NEXTVERSION

        .. seealso:: `del_ancillary`, `get_ancillary`, `ancillaries`

        :Parameters:

            term: `str`
                The name of the term to be set.

            ancillary: (sequence of) `str`
                The ancillary keys for the term.

        :Returns:

            `None`

        """
        if self._get_component("multiple_ancillaries"):
            if isinstance(ancillary, str):
                ancillary = tuple([ancillary])
            else:
                ancillary = tuple(ancillary)

        self._get_component("ancillaries")[term] = ancillary

    def set_multiple_ancillaries(self, multiple_ancillaries):
        """TODOU.

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            multiple_ancillaries: `bool`
                TODOU

        :Returns:

            `None`

        """
        return self._set_component(
            "multiple_ancillaries", bool(multiple_ancillaries), copy=False
        )
