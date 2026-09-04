from .abstract import ParametersAncillaries


class UncertaintyAncillaryParameterisation(ParametersAncillaries):
    """A parameterisation for an uncertainty ancillary construct.
    
    The parameterization formula which describes how the missing data
    array can be created. A term of the parameterization formula can
    be a descriptive string (such as the error-correlation structural
    type "triangular"), or can may be an uncertainty ancillary
    construct (such as one which contains a configuration parameter
    for an error-correlation structural type).

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

               References to uncertainty ancillary constructs. Each
               dictionary key is a parameter name, with corresponding
               value of uncertainty ancillary construct identifier.

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
