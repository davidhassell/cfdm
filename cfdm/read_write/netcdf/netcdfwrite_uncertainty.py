import logging

logger = logging.getLogger(__name__)


class NetCDFWriteUncertainty:
    """Mixin class for writing uncertainty variables to a dataset.

    .. versionadded: (cfdm) NEXTVERSION

    """

    def _create_probability_distribution_attribute(self, f, unc, ncvar):
        """Set the probability_distribution attribute.

        .. versionadded: (cfdm) NEXTVERSION

        :Parameters:

            f: `Field`
                The parent field construct.

            unc: `Uncertainty`
                The uncertainty construct on which to set the
                attribute.

            ncvar: `str`
                The netCDF name of the uncertainty variable.

        :Returns:

            `str` or `None`
                The probability_distribution attribute, or `None` if
                there isn't one.

        """
        g = self.write_vars

        pd = unc.probability_distribution

        # Set the distribution type
        out = []
        for parameter, value in pd.parameters().items():
            if parameter == "distribution":
                out.append(value)
                break

        # Set any distribution parameters
        ancils = pd.ancillaries()
        if ancils:
            out2 = []
            for parameter, keys in ancils.items():
                if parameter == "error_correlation":
                    # Error-correlation parameters go into the
                    # "error_correlation" attribute, which is dealt
                    # with elsewhere.
                    continue

                key = keys[0]
                unc_anc = f.uncertainty_ancillary(key)

                # Get the ncvar of the uncertainty ancillary construct
                # (if there is one)
                anc_ncvar = g["key_to_ncvar"].get(key)

                if anc_ncvar is None:
                    # For some reason, no netCDF variable has been
                    # created for this uncertainty ancillary.
                    logger.warning(
                        f"{unc_anc!r} has not been created as a dataset "
                        "variable"
                    )
                    continue

                out2.append(f"{parameter}: {anc_ncvar}")

            if out2:
                out2 = " ".join(out2)
                out.append(f"({out2})")

        if out:
            return " ".join(out)

    def _create_error_correlation_attribute(self, f, unc, ncvar):
        """Set the error_correlation attribute.

        .. versionadded: (cfdm) NEXTVERSION

        :Parameters:

            f: `Field`
                The parent field construct.

            unc: `Uncertainty`
                The uncertainty construct on which to set the
                attribute.

            ncvar: `str`
                The netCDF name of the uncertainty variable.

        :Returns:

            `str` or `None`
                The error_correlation attribute, or `None` if there
                isn't one.

        """
        g = self.write_vars

        pd = unc.probability_distribution

        out = []
        for parameter, keys in pd.ancillaries().items():
            if parameter != "error_correlation":
                # These parameters go into the
                # "probability_distribution" attribute, which is dealt
                # with elsewhere.
                continue

            for key in keys:
                unc_anc = f.uncertainty_ancillary(key)

                # Add the dataset dimension names
                out.extend(
                    f"{ncdim}:"
                    for ncdim in self._dataset_dimensions(f, key, unc_anc)
                )

                # Get the ncvar of the uncertainty ancillary construct
                # (if there is one)
                anc_ncvar = g["key_to_ncvar"].get(key)

                p = unc_anc.parameterisation

                error_correlation_structure = p.parameters().get(
                    "error_correlation_structure"
                )
                comment = p.parameters().get("comment")

                if anc_ncvar is not None:
                    # The uncertainty ancillary has already been
                    # written to a dataset error-correlation variable.
                    #
                    # Add the error-correlation variable name
                    out.append(anc_ncvar)
                elif error_correlation_structure is not None:
                    # The uncertainty ancillary has NOT been written
                    # to a dataset error-correlation variable, and it
                    # has parameterised data.
                    #
                    # Add the error-correlation structural type
                    out.append(error_correlation_structure)

                out2 = []
                if error_correlation_structure is not None:
                    if comment is not None:
                        comment = f"comment: {comment}"

                    # Error-correlation parameter variables
                    for parameter, key in p.ancillaries().items():
                        unc_anc = f.uncertainty_ancillary(key)

                        # Get the ncvar of the uncertainty ancillary
                        # construct (if there is one)
                        value = g["key_to_ncvar"].get(key)

                        if value is None:
                            # The uncertainty ancillary has NOT been
                            # written to a dataset error-correlation
                            # parameter variable, so encode it inside
                            # the error_correlation attribute as an
                            # integer with units.
                            value = str(anc_ncvar.array.item(0))
                            units = unc_anc.get_property("units", None)
                            if units:
                                value += f" {units}"

                        out2.append(f"{parameter}: {value}")

                if comment is not None:
                    # Always put a comment at the end
                    out2.append(comment)

                if out2:
                    out2 = " ".join(out2)
                    out.append(f"({out2})")

        if out:
            return " ".join(out)

    def _write_uncertainty(self, f, key, unc):
        """Write an uncertainty construct to the dataset.

        Also writes any associated uncertainty ancillaries to the
        dataset.

        .. versionadded: (cfdm) NEXTVERSION

        :Parameters:

            f: `Field`
                The parent field construct.

            key: `str`
                The uncertainty construct identifier
                (e.g. ``'uncertainty1'``).

            unc: `Uncertainty`
                The uncertainty construct.

        :Returns:

            `str`
                The dataset name of the uncertainty variable.

        """
        g = self.write_vars

        ncdimensions = self._dataset_dimensions(f, key, unc)

        create = not self._already_in_file(unc, ncdimensions)

        if not create:
            ncvar = g["seen"][id(unc)]["ncvar"]
        else:
            # --------------------------------------------------------
            # Create the associated uncertainty ancillary variables
            # --------------------------------------------------------
            for (
                parameter,
                keys,
            ) in unc.probability_distribution.ancillaries().items():
                error_correlation = parameter == "error_correlation"
                for ua_key in keys:
                    unc_anc = f.uncertainty_ancillary(ua_key)
                    self._write_uncertainty_ancillary(
                        f,
                        ua_key,
                        unc_anc,
                        error_correlation_parameter=error_correlation,
                    )

            ncvar = self._create_variable_name(unc, default="uncertainty_data")

            # --------------------------------------------------------
            # Create the "probability_distribution" and
            # "error_correlation" attributes (after having created the
            # associated uncertainty ancillary variables).
            # --------------------------------------------------------
            extra = {}
            pd = self._create_probability_distribution_attribute(f, unc, ncvar)
            if pd:
                extra["probability_distribution"] = pd

            ec = self._create_error_correlation_attribute(f, unc, ncvar)
            if ec:
                extra["error_correlation"] = ec

            # --------------------------------------------------------
            # Create a trailing offsets dimension (if required)
            # --------------------------------------------------------
            if unc.ndim < unc.data.ndim:
                size = unc.data.shape[-1]
                ncdim = self._name(
                    unc.nc_get_dimension("offsets"),
                    dimsize=size,
                    role="offsets",
                )
                if ncdim not in g["dimensions"]:
                    self._write_dimension(ncdim, None, size=size)

                ncdimensions = ncdimensions + (ncdim,)

            # Create a new uncertainty variable
            self._write_netcdf_variable(
                ncvar,
                ncdimensions,
                unc,
                self.implementation.get_data_axes(f, key),
                extra=extra,
            )

        g["key_to_ncvar"][key] = ncvar
        g["key_to_ncdims"][key] = ncdimensions

        return ncvar

    def _write_uncertainty_ancillary(
        self, f, key, unc_anc, error_correlation_parameter
    ):
        """Write an uncertainty ancillary construct to the dataset.

        .. versionadded: (cfdm) NEXTVERSION

        :Parameters:

            f: `Field`
                The parent field construct.

            key: `str`
                The uncertainty ancillary construct identifier
                (e.g. ``'uncertaintyancillary1'``).

            unc_anc: `UncertaintyAncillary`
                The uncertainty ancillary construct.

            error_correlation_parameter: `bool`
                True if the uncertainty ancillary construct is an
                error-correlation parameter, otherwise False.

        :Returns:

            `str` or `None`
                The dataset name of the uncertainty ancillary
                variable. If no ancillary ancillary variable was
                written then `None` is returned.

        """
        g = self.write_vars

        ncdimensions = self._dataset_dimensions(f, key, unc_anc)

        create = not self._already_in_file(unc_anc, ncdimensions)

        if not create:
            ncvar = g["seen"][id(unc_anc)]["ncvar"]
        else:
            if (
                error_correlation_parameter
                and unc_anc.nc_get_variable(None) is None
                and not unc_anc.ndim
                and tuple(unc_anc.properties()) in ((), ("units",))
            ):
                # Do not write this error-correlation parameter as a
                # dataset variable (its non-existence as a dataset
                # variable will trigger an encoding as an integer and
                # units inside the "error_correlation" attribute).
                return

            ncvar = self._create_variable_name(
                unc_anc, default="uncertainty_ancillary"
            )

            domain_axes = self.implementation.get_data_axes(f, key)
            extra = None
            if unc_anc.has_dual_dimensions():
                # This is a 2N-d error-correlation matrix, so we need
                # to recast as a 2-d matrix, as per the CF
                # conventions.

                # Update the 'seen' dictionary with the original
                # uncertainty ancillary construct
                g["seen"][id(unc_anc)] = {
                    "variable": unc_anc,
                    "ncvar": ncvar,
                    "ncdims": ncdimensions,
                }

                size = unc_anc.size
                properties = unc_anc.properties()

                # Create dimensions for the 2-d matrix
                ncdim = "_".join(ncdimensions)
                if ncdim not in g["dimensions"]:
                    ncdim = self._name(ncdim)
                    self._write_dimension(ncdim, None, size=size)

                ncdim1 = self._name(f"{ncdim}_1")
                if ncdim1 not in g["dimensions"]:
                    self._write_dimension(ncdim1, None, size=size)

                unc_anc = unc_anc.data.reshape((size,) * 2)
                ncdimensions = (ncdim, ncdim1)
                domain_axes = None
                extra = properties

            # Create a new uncertainty ancillary variable
            self._write_netcdf_variable(
                ncvar, ncdimensions, unc_anc, domain_axes, extra=extra
            )

        g["key_to_ncvar"][key] = ncvar
        g["key_to_ncdims"][key] = ncdimensions

        return ncvar
