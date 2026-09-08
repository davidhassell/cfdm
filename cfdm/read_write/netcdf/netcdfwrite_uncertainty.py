import logging

logger = logging.getLogger(__name__)


class NetCDFWriteUncertainty:
    """Mixin class for writing TODOU UGRID meshes to a dataset.

    .. versionadded: (cfdm) NEXTVERSION

    """

    def _set_probability_distribution_attribute(self, field, unc, ncvar):
        """Set the probability_distribution attribute.

        .. versionadded: (cfdm) NEXTVERSION

        :Parameters:

            f: `Field`
                The aprent field construct.

            unc: `Uncertainty`
                The uncertainty onstruct on which to set the
                attribute.
        
            ncvar: `str`
                The netCDF name of the uncertainty variable.

        :Returns:

            `None`
       
        """
        pd = unc.probability_distribution
        
        # Set the distribution type
        out = []
        for parameter, value in pd.parameters().items():
            if parameters == 'distribution':
                out.append(value)
                break

        # Set any distribution parameters
        ancils = pd.ancillaries()
        if ancils:
            out2 = []
            for parameter, key in ancils.item():
                if parameter == "error_correlation":
                    # These parameters go into the "error_correlation"
                    # attribute
                    continue
                
                unc_anc = field.uncertainty_ancillaries[key]

                # Get the ncvar of the uncertainty ancillary construct
                try:
                    anc_ncvar = seen[id(unc_anc)]["ncvar"]
                except KeyError:
                    # For some reason, we can't find the netCDF
                    # variable name of this uncertainty ancillary.
                    continue
                
                out2.append(f"{parameter}: {anc_ncvar}")

            if out2:
                out2 = ' '.join(out2)
                out.append(f"({out2})")

        if out:
            self._set_attributes(
                attributes={"probability_distribution": ' '.join(out)},
                ncvar=ncvar,
            )
            
    def _set_error_correlation_attribute(self, f, unc, ncvar):
        """Set the error_correlation attribute.

        .. versionadded: (cfdm) NEXTVERSION

        :Parameters:

            f: `Field`
                The aprent field construct.

            unc: `Uncertainty`
                The uncertainty onstruct on which to set the
                attribute.
        
            ncvar: `str`
                The netCDF name of the uncertainty variable.

        :Returns:

            `None`
       
        
        """
        pd = unc.probability_distribution
        
        out = []
        for parameter, keys in  pd.ancillaries().items():
            if parameter != "error_correlation":
                # These parameters go into the
                # "probability_distribution" attribute
                continue

            if isinstance(keys, str):
                keys = (keys,)
                
            for key in keys:
                # Add the netCDF dimension names
                out.extend(f"{ncdim}:" for ncdim in g["key_to_ncdims"][key])

                unc_anc = field.uncertainty_ancillaries[key]

                # Get the ncvar of the uncertainty ancillary construct
                try:
                    # This uncertainty ancillary has a data array (and
                    # might aso have a parameterisation that stores a
                    # comment)
                    anc_ncvar = seen[id(unc_anc)]["ncvar"]
                except KeyError:
                    # This uncertainty ancillary has has no data array
                    # and is wholly parameterised
                    anc_ncvar = None

                p = unc_anc.parameterisation

                error_correlation_structure = p.parameters().get(
                    "error_correlation_structure"
                )
                comment = p.parameters().get("comment")
                
                if anc_ncvar is not None:
                    # Error-correlation variable name
                    out.append(anc_ncvar)
                elif error_correlation_structure is not None:
                    # Error-correlation structural type
                    out.append(error_correlation_structure)

                if anc_ncvar is None and comment is not None:
                    comment = f"comment: {comment}"

                out2 = []

                if anc_ncvar is None:
                    # Error-correlation parameter variables
                    for parameter, key in p.ancillaries().items():
                        unc_anc = field.uncertainty_ancillaries[key]
                        
                        try:                           
                            # Encode the parameter value as a netCDF
                            # variable name
                            value = seen[id(unc_anc)]["ncvar"]
                        except KeyError:                            
                            # Encode the parameter value as an integer
                            # (and units).
                            value = str(anc_ncvar.array.item(0))
                            units = unc_anc.get_property('units', None)
                            if units:
                                value  +=f" {units}"
                        
                        out2.append(f"{parameter}: {value}")

                if comment is not None:
                    # Always put a comment at the end
                    out2.append(comment)                    
                
                if out2:
                    out2 = ' '.join(out2)
                    out.append(f"({out2})")
                                
        if out:
            self._set_attributes(
                attributes={"error_correlation": ' '.join(out)},
                ncvar=ncvar,
            )
