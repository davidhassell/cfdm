import numpy as np

from .abstract import Subarray


class RaggedSubarray(Subarray):
    """A subarray of a compressed ragged array.

    A subarray describes a unique part of the uncompressed array.

    .. versionadded:: (cfdm) 1.9.TODO.0

    """

    def __getitem__(self, indices):
        """Return a subspace of the uncompressed subarray.

        x.__getitem__(indices) <==> x[indices]

        Returns a subspace of the uncompressed subarray as an
        independent numpy array.

        .. versionadded:: (cfdm) 1.9.TODO.0

        """
        d = self._select_data()
        u = self._post_process(d)

        if indices is Ellipsis:
            return u

        return u[indices]

    def _post_process(self, a):
        """TODO.

        .. versionadded:: (cfdm) 1.9.TODO.0

        :Parameters:

            a: `numpy.ndarray`
               TODO

        :Returns:

            `numpy.ndarray`

        """

        d1, u_dims = self.compressed_dimensions.copy().popitem()

        # Get shape from self.compressed_shape (from self.subareas)
        uncompressed_shape = self.shape
        shape = list(a.shape)

        indices = [
            slice(
                None,
            )
        ] * a.ndim
        indices[d1] = slice(0, shape[d1])

        shape[d1] = uncompressed_shape[u_dims[-1]]

        u = np.ma.masked_all(shape, dtype=a.dtype)
        u[tuple(indices)] = a

        u = u.reshape(uncompressed_shape)

        return u

    @property
    def dtype(self):
        """The data-type of the uncompressed data.

        .. versionadded:: (cfdm) 1.9.TODO.0

        """
        return self.data.dtype
