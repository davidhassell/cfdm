from .abstract import ParametersAncillaries


class ProbabilityDistribution(ParametersAncillaries):
    """TODOU o collect named parameters andTODOU domain ancillaries.

    .. versionadded:: (cfdm) NEXTVERSION

    """

    def __init__(
        self,
        distribution=None,
        parameters=None,
        uncertainty_ancillaries=None,
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

               *Example:*
                 ``parameters={'distribution_name': 'gaussian'}``

            uncertainty_ancillaries: `dict`, optional
               Set references to uncertainty ancillary constructs. The
               dictionary keys are parameter names, with corresponding
               values of construct keys. A named parameters may have a
               value of a single key, or multiple keys.

               References to uncertainty ancillary constructs may also
               be set after initialisation with the `set_ancillaries`,
               and `set_ancillary` methods.

               *Example:*
                 ``uncertainty_ancillaries={'skew':
                 'uncertaintyancillary2', 'error_correlations':
                 ('uncertaintyancillary1', uncertaintyancillary2')}``

            {{init source: optional}}

            {{init copy: `bool`, optional}}

        """
        super().__init__(
            parameters=parameters,
            ancillaries=uncertainty_ancillaries,
            multiple_ancillaries=True,
            source=source,
            copy=copy,
        )
