from ..decorators import _inplace_enabled, _inplace_enabled_define_and_cleanup


class Topology:
    """Mixin class for topology-related constructs.

    .. versionadded:: (cfdm) UGRIDVER

    """

    @classmethod
    def _normalise_cell_ids(cls, data, start_index):
        """Normalise cell identifier values.

        Normalised data is in a form that is suitable for creating a
        CF-netCDF UGRID connectivity variable.

        See `normalise` for further details.

        .. versionadded:: (cfdm) UGRIDVER

        .. seealso:: `normalise`

        :Parameters:

            data: `numpy.ndarray`
                The original numpy array of cell identifiers.

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

        # Replace the original (now positive) cell ids with negative
        # numbers
        #
        # PERFORMANCE WARNING: This loop could be slow
        place = np.place
        for i, j in zip(ids.tolist(), range(-ids.size, 0)):
            place(data, data == i, j)

        if masked:
            data = np.ma.array(data, mask=mask)

        # Remove redundant cell ids
        if data.max() > 0:
            data = np.ma.where(data > 0, np.ma.masked, data)

        if np.ma.is_masked(data):
            # Move missing values to the end of each row
            data[:, 1:].sort(axis=1, endwith=True)

            # Discard columns that are all missing data
            count = data.count(axis=0)
            if not count.min():
                index = np.where(count)[0]
                data = data[:, index[0] : index[-1] + 1]

        # Convert the new negative values to non-negative values
        if start_index:
            data += ids.size + 1
        else:
            data += ids.size

        return data

    @_inplace_enabled(default=False)
    def transpose(self, axes=None, inplace=False):
        """Permute the axes of the data array.

        In this context, the data only has one axis, the first
        one. The second data dimension can not be moved and may not be
        specified. Therefore, the `transpose` method never changes the
        data.

        .. versionadded:: UGRIDVER

        .. seealso:: `insert_dimension`, `squeeze`

        :Parameters:

            axes: (sequence of) `int`, optional
                The new axis order. By default the order is reversed.

                {{axes int examples}}

            {{inplace: `bool`, optional}}

        :Returns:

            `{{class}}` or `None`
                The new construct with permuted data axes. If the
                operation was in-place then `None` is returned.

        """
        if axes is None:
            iaxes = list(range(self.ndim - 1, -1, -1))
        else:
            iaxes = self._parse_axes(axes)

        if iaxes != [0]:
            raise ValueError(
                f"Can't transpose {self.__class__.__name__} with axes "
                f"of {axes!r}. Axes must be equivalent to [0]"
            )

        c = _inplace_enabled_define_and_cleanup(self)
        super(Topology, c).transpose(iaxes + [-1], inplace=True)
        return c
