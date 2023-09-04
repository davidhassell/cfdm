class Topology:
    """Mixin class for topology constructs.

    .. versionadded:: (cfdm) TODOUGRIDVER

    """

    @classmethod
    def _normalise_cell_ids(cls, data, start_index):
        """Normalise cell identifier values.

        Normalised data is in a form that is suitable for creating a
        CF-netCDF UGRID connectivity variable.

        See `normalise` for further details.

        .. versionadded:: (cfdm) TODOUGRIDVER

        .. seealso:: `normalise`

        :Parameters:

            data: `np.ndarray`
                TODOUGRID

            start_index: `int`, optional
                The start index for the data values in the normalised
                data. Must be ``0`` or ``1`` for zero- or one-based
                indices respectively.

        :Returns:

            `numpy.ndarray`
                The normailised data.

        **Examples*

        See `normalise` for examples.

        """
        import numpy as np

        masked = np.ma.is_masked(data)
        if masked:
            mask = data.mask
        
        # Remove negative values
        dmin = data.min()
        if dmin < 0:
            data -= dmin

        # Get the original cell ids
        ids = data[:, 0]

        where = np.where
        for i, j in zip(ids.tolist(), range(-ids.size, 0)):
            data = where(data == i, j, data)

        if masked:
            data = np.ma.array(data, mask=mask)

        # Remove redundant cell ids
        if data.max() > 0:
            data = np.ma.where(data > 0, np.ma.masked, data)

        if np.ma.is_masked(data):
            # Move missing values to the end of each row
            data[:, 1:].sort(axis=1, endwith=True)

            # Discard columns that are all missing data
            count = data.count(axis=0)[0]
            if not count.min():
                index = np.where(count)[0]
                data = data[:, index[0] : index[-1] + 1]

        # Convert the new negative values to non-negative values
        if start_index:
            data += ids.size + 1
        else:
            data += ids.size

        return data
