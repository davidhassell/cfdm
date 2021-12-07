from .abstract import SubsampledArray
from .mixin import BiLinearInterpolation


class SubsampledBiLinearSubarray(BiLinearInterpolation, SubsampledArray):
    """TODO

    .. versionadded:: (cfdm) 1.9.TODO.0

    """

    def __getitem__(self, indices):
        """Return a subspace of the uncompressed data.

        x.__getitem__(indices) <==> x[indices]

        Returns a subspace of the uncompressed data as an independent
        numpy array.

        .. versionadded:: (cfdm) 1.9.TODO.0

        """
        (d1, d2) = self.subsampled_dimensions

        u = self._bilinear_interpolation(
            ua=self._select_tie_point(location={d1: 0, d2: 0}),
            ub=self._select_tie_point(location={d1: 0, d2: 1}),
            uc=self._select_tie_point(location={d1: 1, d2: 0}),
            ud=self._select_tie_point(location={d1: 1, d2: 1}),
            d1=d1,
            d2=d2,
        )
        u  = self._post_process(u)
            
        if indices is Ellipsis:
            return u
        
        return self.get_subspace(u, indices, copy=True)
