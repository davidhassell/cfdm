from .abstract import ParametersAncillaries


class UncertaintyAncillaryParameterisation(ParametersAncillaries):
    """TODOU o colA parametrisation for an uncertainty ancillary data array.

    TODOU

    .. versionadded:: (cfdm) NEXTVERSION

    """

    def __init__(
        self,
        parameters=None,
        uncertainty_ancillaries=None,
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
                 ``parameters={'error_correlation_structure': 'triangular'}``

            uncertainty_ancillaries: `dict`, optional
               Set references to uncertainty ancillary constructs. The
               dictionary keys are parameter names, with corresponding
               values of construct keys.

               References to uncertainty ancillary constructs may also
               be set after initialisation with the `set_ancillaries`,
               and `set_ancillary` methods.

               *Example:*
                 ``uncertainty_ancillaries={'e_folding_length':
                 'uncertaintyancillary2'}``

            {{init source: optional}}

            {{init copy: `bool`, optional}}

        """
        super().__init__(
            parameters=parameters,
            ancillaries=uncertainty_ancillaries,
            multiple_ancillaries=False,
            source=source,
            copy=copy,
        )
