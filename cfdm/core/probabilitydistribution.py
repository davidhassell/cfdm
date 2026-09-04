from .abstract import ParametersAncillaries


class ProbabilityDistribution(ParametersAncillaries):
    """A probability distribution for an uncertainty construct.

    The coverage interval of an uncertainty construct is derived from
    the probability distribution. The probability distribution is
    defined by the values of named parameters. A parameter value can
    be a descriptive string (such as the distribution type
    "gaussian"), or an uncertainty ancillary construct (such as one
    containing spatially varying skewness data), or one or more
    uncertainty ancillary constructs (such as multiple uncertainty
    ancillary constructs containing error-correlation data for
    non-overlapping subsets of the domain axis constructs).

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
               References to uncertainty ancillary constructs. Each
               dictionary key is a parameter name, with corresponding
               value of uncertainty ancillary construct identifiers. A
               named parameter may have a value of a single key, or a
               sequence of keys.

               References to uncertainty ancillary constructs may also
               be set after initialisation with the `set_ancillaries`,
               and `set_ancillary` methods.

               *Example:*
                 ``uncertainty_ancillaries={'skew':
                 'uncertaintyancillary2', 'error_correlation':
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
