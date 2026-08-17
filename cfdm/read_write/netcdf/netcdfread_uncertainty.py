import logging

logger = logging.getLogger(__name__)


class NetCDFReadUncertainty:
    """Mixin class for reading uncertainty metadata from a dataset.

    .. versionadded: (cfdm) NEXTVERSION

    """

    def _create_uncertainty(self, field_ncvar, ncvar):
        """Create an uncertainty construct.

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            field_ncvar: `str`
                The netCDF name of the parent data variable.

            ncvar: `str`
                The netCDF name of the uncertainty variable.

        :Returns:

            `Uncertainty`

        """
        g = self.read_vars

        properties = g["variables"][ncvar].attrs.copy()

        # Create an empty uncertainty construct
        uncertainty = self.implementation.initialise_Uncertainty()

        # ------------------------------------------------------------
        # Probability distribution
        # ------------------------------------------------------------
        probabilty_distribution = properties.pop(
            "probabilty_distribution", None
        )
        if probabilty_distribution:
            parsed_probabilty_distribution = (
                self._parse_probabilty_distribution(probabilty_distribution)
            )
            ok = self._check_probabilty_distribution(
                field_ncvar,
                ncvar,
                probabilty_distribution,
                parsed_probabilty_distribution,
            )
            if ok:
                distribution = parsed_probabilty_distribution["distribution"]
                if distribution is not None:
                    uncertainty.probability_distribution.set_parameter(
                        "distribution", distribution
                    )

                for parameter, dp_ncvar in parsed_probabilty_distribution[
                    "parameters"
                ].items():
                    ok = self._check_distribution_parameter_variable(
                        field_ncvar,
                        ncvar,
                        "probability_distribution",
                        dp_ncvar,
                    )
                    if not ok:
                        continue

                    if dp_ncvar in g["uncertainty_ancillary"]:
                        unc_anc = g["uncertainty_ancillary"][dp_ncvar].copy()
                    else:
                        unc_anc = self._create_uncertainty_ancillary(dp_ncvar)

                    axes = self._get_domain_axes(
                        dp_ncvar, parent_ncvar=field_ncvar
                    )

                    # Insert the uncertainty ancillary
                    logger.detail(
                        "        [o] Inserting distribution parameter "
                        f"{unc_anc!r}"
                    )  # pragma: no cover
                    key = field.set_construct(unc_anc, axes=axes, copy=False)
                    self._reference(dp_ncvar, field_ncvar)

                    g["uncertainty_ancillary"][dp_ncvar] = unc_anc

                    uncertainty.probability_distribution.set_ancillary(
                        parameter, key
                    )

        # ------------------------------------------------------------
        # Error correlation
        # ------------------------------------------------------------
        error_correlation = properties.pop("error_correlation", None)
        if error_correlation:
            parsed_error_correlation = self._parse_error_correlation(
                error_correlation
            )
            ok = self._check_error_correlation(
                field_ncvar,
                ncvar,
                error_correlation,
                parsed_error_correlation,
            )
            if ok:
                ncdim_to_axis = g["ncdim_to_axis"]

                error_correlation_keys = []

                for element in parsed_error_correlation:
                    # Get, from the parsed error_correlation
                    # attribute, the domain axes for the
                    # error-correlation uncertainty ancillary
                    # construct
                    axes = [
                        ncdim_to_axis[ncdim]
                        for ncdim in element["dimensions"]
                        if ncdim in ncdim_to_axis
                    ]

                    ecp_ncvar = element["error_correlation_variable"]
                    if ecp_ncvar is not None:
                        # Create an uncertainty ancillary construct
                        # from a CF error-correlation parameter
                        # variable
                        ok = self._check_error_correlation_variable(
                            field_ncvar, ncvar, "error_correlation", ecp_ncvar
                        )
                        if not ok:
                            continue

                        if ecp_ncvar in g["uncertainty_ancillary"]:
                            unc_anc = g["uncertainty_ancillary"][
                                ecp_ncvar
                            ].copy()
                        else:
                            unc_anc = self._create_uncertainty_ancillary(
                                ecp_ncvar, trailing_dimensions=True
                            )
                            g["uncertainty_ancillary"][ecp_ncvar] = unc_anc

                        # Insert the uncertainty ancillary
                        logger.detail(
                            f"        [p] Inserting {unc_anc!r}"
                        )  # pragma: no cover
                        key = field.set_construct(
                            unc_anc, axes=axes, copy=False
                        )
                        self._reference(ecp_ncvar, field_ncvar)

                        error_correlation_keys.append(key)
                        continue

                    # Still here? Then create an error-correlation
                    # uncertainty ancillary construct without a
                    # corresponding CF error-correlation variable.
                    unc_anc = (
                        self.implementation.initialise_UncertaintyAncillary(
                            trailing_dimensions=True
                        )
                    )
                    comment = element["comment"]
                    if comment:
                        # Store the comment as a property of the
                        # uncertainty ancillary construct
                        unc_anc.set_property("comment")

                    error_correlation_structure = element[
                        "error_correlation_structure"
                    ]
                    if error_correlation_structure is None:
                        # The uncertainty ancillary construct has no
                        # data array, nor a parameterised data array.
                        logger.detail(
                            "        [q] Inserting error-correlation "
                            f"{unc_anc!r}"
                        )  # pragma: no cover
                        key = field.set_construct(
                            unc_anc, axes=axes, copy=False
                        )

                        error_correlation_keys.append(key)
                        continue

                    # Still here? Then the uncertainty ancillary
                    # construct has a parameterised data array.
                    unc_anc.parameterisation.set_parameter(
                        "error_correlation_structure",
                        error_correlation_structure,
                    )

                    # Loop round the error-correlation parameters
                    for parameter, value in element["parameters"].items():
                        if isinstance(value, int):
                            # The error-correlation parameter is
                            # defined by a dimensionless integer
                            # originating from the "error_correlation"
                            # attribute, i.e. there is no CF
                            # error-correlation parameter variable in
                            # the dataset.
                            ecp_ncvar = None
                            axes = ()
                        else:
                            # The error-correlation parameter is
                            # defined by a CF error-correlation
                            # parameter variable in the dataset.
                            ecp_ncvar = value
                            ok = self._check_error_correlation_parameter_variable(
                                field_ncvar,
                                ncvar,
                                "error_correlation",
                                ecp_ncvar,
                            )
                            if not ok:
                                continue

                            ecp_axes = self._get_domain_axes(
                                ncvar, parent_ncvar=field_ncvar
                            )
                            value = None

                        ecp_unc_anc = self._create_uncertainty_ancillary(
                            ncvar, data=value
                        )

                        # Insert the uncertainty ancillary
                        logger.detail(
                            "        [r] Inserting error-correlation "
                            f"parameter {ecp_unc_anc!r}"
                        )  # pragma: no cover
                        key_ecp = field.set_construct(
                            ecp_unc_anc, axes=ecp_axes, copy=False
                        )
                        self._reference(ncvar, field_ncvar)

                        unc_anc.parameterisation.set_ancillary(
                            parameter, key_ecp
                        )

                    logger.detail(
                        f"        [s] Inserting error-correlation {unc_anc!r}"
                    )  # pragma: no cover
                    key = field.set_construct(unc_anc, axes=axes, copy=False)

                    error_correlation_keys.append(key)

                if error_correlation_keys:
                    uncertainty.probability_distribution.set_ancillary(
                        "error_correlation", error_correlation_keys
                    )

        # Insert properties (having removed any
        # probabilty_distribution and error_correlation attributes)
        self.implementation.set_properties(uncertainty, properties, copy=False)

        if not self.read_vars["mask"]:
            self._set_default_FillValue(uncertainty, ncvar)

        # Insert data
        data = self._create_data(ncvar, uncertainty, parent_ncvar=field_ncvar)
        self.implementation.set_data(uncertainty, data, copy=False)

        # Store the netCDF variable name
        self.implementation.nc_set_variable(uncertainty, ncvar)

        trailing_dimension=properties.get('coverage_interval') == "offsets"
        if trailing_dimension:
            # Set the netCDF trailing dimension name
            try:
                ncdim = g["variable_dimension_paths"][ncvar][-1]
            except IndexError:
                pass
            else:
                unc_anc.nc_set_dimension(ncdim)

        # Store the original file names
        self.implementation.set_original_filenames(
            uncertainty,
            g["variables"][ncvar].dataset,
        )

        # Set quantization metadata
        self._set_quantization(uncertainty, ncvar)

        return uncertainty

    def _create_uncertainty_ancillary(
            self, ncvar, trailing_dimensions=False, data=None
    ):
        """Create an uncertainty ancillary construct.
        
        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            ncvar: `str` or `None`
                The name of the CF uncertainty ancillary variable, or
                `None` if there is no variable (because the construct
                is inferred from an attribute, such as
                ``error_correlation``).

            trailing_dimensions: `bool`, optional
                True if the uncertainty ancillary construct has extra
                trailing dimensions.

            data: optional
                Provide the data array. If `None` then the array is
                taken from the variable in the dataset.
        
        :Returns:

            `UncertaintyAncillary`

        """
        g = self.read_vars        
        
        # Create an empty uncertainty construct
        unc_anc = self.implementation.initialise_UncertaintyAncillary(
            trailing_dimensions=bool(trailing_dimensions)
        )

        if ncvar is not None:
            # Insert properties
            self.implementation.set_properties(
                unc_anc,
                g["variables"][ncvar].attrs,
                copy=True,
            )
            
            if not self.read_vars["mask"]:
                self._set_default_FillValue(field_ancillary, ncvar)

            if trailing_dimensions:
                # Set the netCDF trailing dimension name
                try:
                    ncdim = g["variable_dimension_paths"][ncvar][-1]
                except IndexError:
                    pass
                else:
                    unc_anc.nc_set_dimension(ncdim)

        if data is not None:
            # Create data from the given value
            data = self.implementation.initialise_Data(
                array=data, copy=False
            )
            unc_anc.nc_set_data_in_attribute(True)             
        elif ncvar is not None:
            # Create data from the variable in the dataset
            data = self._create_data(ncvar, unc_anc)

        # Insert data
        self.implementation.set_data(unc_anc, data, copy=False)

        # Store the original file names
        self.implementation.set_original_filenames(
            unc_anc,
            g["variables"][ncvar].dataset,
        )

    def _parse_probability_distribution(self, probability_distribution):
        """Parse a CF probability_distribution string.

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            probability_distribution: `str`
                A CF probability_distribution string.

        :Returns:

            `dict`

        **Examples**

        >>> _parse_probability_distribution('')
        {'distribution': None,
         'parameters': {}}
        >>> _parse_probability_distribution('gaussian')
        {'distribution': 'gaussian',
         'parameters': {}}
        >>> _parse_probability_distribution('skewed_gaussian (skew: varname)')
        {'distribution': 'skewed_gaussian',
         'parameters': {'skew: 'varname'}}

        """
        # ------------------------------------------------------------
        # Split the probability_distribution string into a list of
        # strings ready for parsing. For example:
        #
        #   '' would be split into: []
        #
        #   'gaussian' would be split into: ['gaussian']
        #
        #   'skewed_gaussian (skew: varname)' would be split up into:
        #   ['skewed_gaussian', '(', 'skew:', 'varname', ')']
        # ------------------------------------------------------------
        probability_distribution = self._split_by_space_and_round_brackets(
            probability_distribution
        )

        empty = {"distribution": None, "parameters": {}}
        out = deepcopy(empty)

        previous = None
        for x in probability_distribution:
            if x == ")":
                break

            if previous is None:
                out["distribution"] = x
                previous = "distribution"
                continue

            if previous == "distribution":
                if x != "(":
                    return empty

                previous = "("
                continue

            if previous in ("(", "value"):
                if not x.endswith(":"):
                    return empty

                parameter = x[:-1]
                previous = "parameter"
                continue

            if previous == "parameter":
                out["parameters"][parameter] = x
                previous = "value"
                parameter = None
                continue

            # Still here? Then it must be a badly formatted string.
            return empty

        return out

    def _parse_error_correlation(self, error_correlation):
        """Parse a CF error_correlation string.

        .. versionadded:: (cfdm) NEXTVERSION

        :Parameters:

            error_correlation: `str`
                A CF error_correlation string.

        :Returns:

            `list` of `dict`

        **Examples**

        >>> _parse_error_correlation('')
        []
        >>> _parse_error_correlation('lat: lon: varname')
        [{'dimensions': ['lat, 'lon'],
          'error_correlation_variable': 'varname',
          'error_correlation_structure': None,
          'comment': '',
          'parameters': {}}]
        >>> _parse_error_correlation('lat: varname (info 1) lon: triangular')
        [{'dimensions': ['lat],
          'error_correlation_variable': 'varname',
          'error_correlation_structure': None,
          'comment': 'info 1',
          'parameters': {}},
         {'dimensions': ['lon],
          'error_correlation_variable': None,
          'error_correlation_structure': 'triangular',
          'comment': '',
          'parameters': {}}]
        >>> _parse_error_correlation('lon: triangular (e_folding_length: var localization_radius: 10 comment: info 2), time: z: (info 3)')
        [{'dimensions': ['lon],
          'error_correlation_variable': None,
          'error_correlation_structure': 'triangular',
          'comment': 'info 2',
          'parameters': {'e_folding_length': 'var',
                         'localization_radius': 10}},
         {'dimensions': ['time', 'z'],
          'error_correlation_variable': None,
          'error_correlation_structure': None,
          'comment': 'info 3',
          'parameters': {}}]

        """
        import re

        g = self.read_vars

        # ------------------------------------------------------------
        # Split the probability_distribution string into a list of
        # strings ready for parsing. For example:
        #
        #   'lat: lon: varname (comment)' would be split up into:
        #   ['lat:', 'lon:', 'varname', '(', 'comment', ')']
        # ------------------------------------------------------------
        error_correlation = self._split_by_space_and_round_brackets(
            error_correlation
        )

        if not error_correlation:
            return []

        form = None
        parameter = None
        previous = None

        out = []
        empty = {
            "dimensions": [],
            "error_correlation_variable": None,
            "error_correlation_structure": None,
            "comment": [],
            "parameters": {},
        }

        element = deepcopy(empty)

        for x in error_correlation:
            if previous in (None, "dimension", ")") and x.endswith(":"):
                # Store a dimension
                element["dimensions"].append(x[:-1])
                previous = "dimension"
                continue

            if previous == "dimension" and not x.endswith(":"):
                if x == "(":
                    form = 3
                    previous = "("
                else:
                    if x in g["variables"]:
                        # An error-correlation variable name
                        form = 1
                        element["error_correlation_variable"] = x
                    else:
                        # An error-correlation structure name
                        form = 2
                        element["error_correlation_structure"] = x

                    previous = "name"

                continue

            if previous == "name":
                if x == "(":
                    previous = "("
                    continue

                if x.endswith(":"):
                    # Store a dimension in a new element
                    out.append(element)
                    element = deepcopy(empty)
                    form = None

                    element["dimensions"].append(x[:-1])
                    previous = "dimension"
                    continue

            if previous == "(" and form in (1, 3):
                if x == "comment:":
                    previous = "comment"
                    continue

                element["comment"].append(x)
                previous = "comment"
                continue

            if previous == "(" and form == 2:
                if x == "comment:":
                    previous = "comment"
                elif x.endswith(":"):
                    parameter = x[:-1]
                    previous = "parameter"

                continue

            if previous == "comment":
                if x == ")":
                    previous = ")"
                    # Start a new element
                    form = None
                    out.append(element)
                    element = deepcopy(empty)
                else:
                    # Part of the comment
                    out["comment"].append(x)
                    previous = "comment"

                continue

            if previous == "parameter":
                if x == ")":
                    # Missing parameter value
                    x = None
                    # Start a new element
                    previous = ")"
                    form = None
                    out.append(element)
                    element = deepcopy(empty)
                else:
                    # A parameter value
                    if x not in g["variables"] and re.match("^\d+$", x):
                        x = int(x)

                    previous = "parameter value"

                element["parameters"][parameter] = x
                parameter = None
                continue

            if previous == "parameter value":
                if x == "comment:":
                    # A comment
                    previous = "comment"
                    continue

                if x.endswith(":"):
                    # Another parameter
                    parameter = x[:-1]
                    previous = "parameter"
                    continue

                if x == ")":
                    # End of parameters and comment
                    previous = ")"
                    # Start a new element
                    out.append(element)
                    element = deepcopy(empty)
                    continue

            # Still here? Then it must be a badly formatted string.
            return []

        # Concatenate 'comment' parts
        for element in out:
            element["comment"] = " ".join(element["comment"])

        return out
