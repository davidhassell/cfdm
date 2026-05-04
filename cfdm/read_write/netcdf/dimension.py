# class Dimension:
#    """A named dimension.
#
#    .. versionadded:: (cfdm) NEXTVERSION
#
#    """
#
#    def __init__(self, name, size, group, unlimited):
#        """**Initialisation**
#
#        :Parameters:
#
#            name: `str`
#                The dimension name.
#
#            size: `int`
#                The dimension size.
#
#            group:
#                The group that the dimension is a member of.
#
#            root_group:
#                The root group.
#
#        """
#        self.name = name
#        self.size = size
#        self._group = group
#        self._unlimited = bool(unlimited)
#
#    def __len__(self):
#        """The size of the dimension.
#
#        x.__len__() <==> len(x)
#
#        .. versionadded:: (cfdm) NEXTVERSION
#
#        """
#        return self.size
#
#    def group(self):
#        """Return the group that the dimension is a member of.
#
#        .. versionadded:: (cfdm) NEXTVERSION
#
#        :Returns:
#
#                The group containing the dimension.
#
#        """
#        return self._group
#
#    def isunlimited(self):
#        """Whether or not the dimension is unlimited.
#
#        .. versionadded:: (cfdm) NEXTVERSION
#
#        :Returns:
#
#            `bool`
#                `True` if and only if the dimension is unlimited.
#
#        """
#        return self._unlimited
