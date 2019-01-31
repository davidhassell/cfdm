from builtins import super

from . import abstract
from . import mixin

from . import Constructs


class Field(mixin.ConstructAccess, abstract.PropertiesData):
    '''A field construct of the CF data model.

The field construct is central to the CF data model, and includes all
the other constructs. A field corresponds to a CF-netCDF data variable
with all of its metadata. All CF-netCDF elements are mapped to a field
construct or some element of the CF field construct. The field
construct contains all the data and metadata which can be extracted
from the file using the CF conventions.

The field construct consists of a data array and the definition of its
domain (that describes the locations of each cell of the data array),
field ancillary constructs containing metadata defined over the same
domain, and cell method constructs to describe how the cell values
represent the variation of the physical quantity within the cells of
the domain. The domain is defined collectively by the following
constructs of the CF data model: domain axis, dimension coordinate,
auxiliary coordinate, cell measure, coordinate reference and domain
ancillary constructs. All of the constructs contained by the field
construct are optional.

The field construct also has optional properties to describe aspects
of the data that are independent of the domain. These correspond to
some netCDF attributes of variables (e.g. units, long_name and
standard_name), and some netCDF global file attributes (e.g. history
and institution).

.. versionadded:: 1.7.0

    '''

    # Define the base of the identity keys for each construct type
    _construct_key_base = {'auxiliary_coordinate': 'auxiliarycoordinate',
                           'cell_measure'        : 'cellmeasure',
                           'cell_method'         : 'cellmethod',
                           'coordinate_reference': 'coordinatereference',
                           'dimension_coordinate': 'dimensioncoordinate',
                           'domain_ancillary'    : 'domainancillary',
                           'domain_axis'         : 'domainaxis',
                           'field_ancillary'     : 'fieldancillary',
    }
    
    def __new__(cls, *args, **kwargs):
        instance = super().__new__(cls)
        instance._Constructs = Constructs
        return instance
    
#        obj = object.__new__(cls, *args, **kwargs)
#        obj._Constructs = Constructs
#        return obj
    #--- End: def

    def __init__(self, properties={}, source=None, copy=True,
                 _use_data=True):
        '''**Initialization**
        
:Parameters:

    properties: `dict`, optional
        Set descriptive properties. The dictionary keys are property
        names, with corresponding values. Ignored if the *source*
        parameter is set.

        *Parameter example:*
           ``properties={'standard_name': 'air_temperature'}``
        
        Properties may also be set after initialisation with the
        `properties` and `set_property` methods.

    source: optional
        Initialize the properties, data and metadata constructs from
        those of *source*.
        
    copy: `bool`, optional
        If False then do not deep copy input parameters prior to
        initialization. By default arguments are deep copied.

        '''
        super().__init__(properties=properties, source=source,
                         copy=copy, _use_data=False)

        if source is not None:
            # Initialise constructs and the data from the source
            # parameter
            try:
#                constructs = source._get_constructs(None)
                constructs = source.constructs
            except AttributeError:
                constructs = None
                
            try:
                data = source.get_data(None) 
            except AttributeError:
                data = None

            try:
                data_axes = source.get_data_axes(None)
            except AttributeError:
                data_axes = None
                                
            if constructs is not None and (copy or not _use_data):
                constructs = constructs.copy(data=_use_data)
        else:
            constructs = None
            data       = None
            data_axes  = None

        if constructs is None:
            constructs = self._Constructs(**self._construct_key_base)
            
        self._set_component('constructs', constructs, copy=False)

        if data is not None and _use_data:
            self.set_data(data, data_axes, copy=copy)
        elif data_axes is not None:
            self.set_data_axes(data_axes)
     #--- End: def
    
    # ----------------------------------------------------------------
    # Private methods
    # ----------------------------------------------------------------
    def _get_constructs(self, *default):
        '''Return the `Constructs` instance contained by the field construct.

.. versionadded:: 1.7.0

:Parameters:

    default: optional
        Return *default* if the `Constructs` instance has not been
        set.

:Returns:

    `Constucts`

**Examples:**

>>> c = f._get_contructs()

        '''
        return self._get_component('constructs', *default)
    #--- End: def

    # ----------------------------------------------------------------
    # Attributes
    # ----------------------------------------------------------------
    @property
    def construct_type(self):
        '''Return a description of the construct type.
        
.. versionadded:: 1.7.0
        
:Returns:

    `str`
        The construct type.

        '''
        return 'field'
    #--- End: def        

    @property
    def constructs(self):
        '''<TODO>
        '''
        return self._get_constructs()
    #--- End: def

    @property
    def domain(self):
        '''Return the domain.
        
``f.domain`` is equivalent to ``f.get_domain()`` 

.. versionadded:: 1.7.0

.. seealso:: `get_domain`

:Returns:

    `Domain`
        The domain.

**Examples:**

>>> d0 = f.domain
>>> d1 = f.get_domain()
>>> d0.equals(d1)
True
 
        '''
        return self.get_domain()
    #--- End: def
    
    # ----------------------------------------------------------------
    # Methods
    # ----------------------------------------------------------------
    def del_data_axes(self, *default):
        '''Remove the keys of the domain axes spanned by the data array.

.. versionadded:: 1.7.0

.. seealso:: `data`, `get_data_axes`, `set_data_axes`

:Parameters:

    default: optional
        Return *default* if data axes have not been set.

:Returns:

    `tuple`
        The removed keys of the domain axes spanned by the data
        array. If unset then *default* is returned, if provided.

**Examples:**

>>> f.set_data_axes(['domainaxis0', 'domainaxis1'])
>>> f.get_data_axes()
('domainaxis0', 'domainaxis1')
>>> f.del_data_axes()
('domainaxis0', 'domainaxis1')
>>> print(f.del_dataxes(None))
None
>>> print(f.get_data_axes(None))
None

        '''
        return self._del_component('data_axes', *default)
    #--- End: def

    def get_domain(self):
        '''Return the domain.

.. versionadded:: 1.7.0

.. seealso:: `domain`

:Returns:

    `Domain`
        TODO

**Examples:**

>>> d = f.get_domain()

        '''
        return self._Domain.fromconstructs(self._get_constructs())
    #--- End: def

    def get_data_axes(self, key=None, default=ValueError):
        '''Return the keys of the domain axes spanned by a data array.

.. versionadded:: 1.7.0

.. seealso:: `data`, `del_data_axes`, `get_data`, `set_data_axes`

:Parameters:

    key: `str`, optional
        <TODO>

    default: optional
        <TODO> Return *default* if data axes have not been set.

:Returns:

    `tuple` 
        The identifiers of the domain axes spanned by the data
        array. If unset then *default* is returned, if provided.

**Examples:**

>>> f.set_data_axes(['domainaxis0', 'domainaxis1'])
>>> f.get_data_axes()
('domainaxis0', 'domainaxis1')
>>> f.del_data_axes()
('domainaxis0', 'domainaxis1')
>>> print(f.del_dataxes(None))
None
>>> print(f.get_data_axes(default=None))
None

        '''
        if key is not None:
            try:
                return self.constructs.data_axes()[key]
            except KeyError:
                return self._default(default, message='2736492783 e28037')
                
        return self._get_component('data_axes', default)
    #--- End: def
    
    def del_construct(self, key):
        '''Remove a metadata construct.

.. versionadded:: 1.7.0

.. seealso:: `get_construct`, `constructs`

:Parameters:

    key: `str`
        The construct identifier of the metadata construct to be
        removed.

        *Parameter example:*
          ``key='auxiliarycoordinate0'``
        
:Returns:

    metadata construct
        The removed metadata construct.

**Examples:**

>>> f.del_construct('auxiliarycoordinate0')

        '''
        if key in self.get_data_axes(default=()):
            raise ValueError(
                "Can't remove domain axis {!r} that is spanned by the field's data".format(key))

        return super().del_construct(key)
    #--- End: def

    def set_data(self, data, axes, copy=True):
        '''Set the data.

The units, calendar and fill value properties of the data object are
removed prior to insertion.

.. versionadded:: 1.7.0

.. seealso:: `data`, `del_data`, `get_data`, `has_data`,
             `set_data_axes`

:Parameters:

    data: `Data`
        The data to be inserted.

    axes: sequence of `str` or `None`, optional
        The identifiers of the domain axes spanned by the data
        array. If `None` then the data axes are not set.

        The axes may also be set afterwards with the `set_data_axes`
        method.

    copy: `bool`, optional
        If False then do not copy the data prior to insertion. By
        default the data are copied.

:Returns:

    `None`

**Examples:**

TODO

        '''
        if axes is not None:
            self.set_data_axes(axes)

        super().set_data(data, copy=copy)
    #--- End: def

    def set_data_axes(self, axes, key=None):
        '''Set the domain axes spanned by the data array.

.. versionadded:: 1.7.0

.. seealso:: `data`, `del_data_axes`, `get_data`, `get_data_axes`

:Parameters:

     axes: sequence of `str`
        The identifiers of the domain axes spanned by the data array.

        *Parameter example:*
          ``axes=['domainaxis0', 'domainaxis1']``

:Returns:

    `None`

**Examples:**

>>> f.set_data_axes(['domainaxis0', 'domainaxis1'])
>>> f.get_data_axes()
('domainaxis0', 'domainaxis1')
>>> f.del_data_axes()
('domainaxis0', 'domainaxis1')
>>> print(f.del_dataxes(None))
None
>>> print(f.get_data_axes(None))
None

        '''
        if key is None:
            #        domain_axes = self.constructs(construct='domain_axis')
            domain_axes = self.constructs.select(construct='domain_axis')
            for axis in axes:
                if axis not in domain_axes:
                    raise ValueError(
"Can't set data axes: Domain axis {!r} doesn't exist".format(axis))
            # <TODO> check shape
            self._set_component('data_axes', tuple(axes), copy=False)
        else:
            self.constructs._set_construct_data_axes(key=key, axes=axes)
    #--- End: def
    
#    def cell_methods(self, copy=False):
#        '''
#        '''
#        out = self.Constructs.cell_methods(copy=copy)
#
#        if not description:
#            return self.Constructs.cell_methods()
#        
#        if not isinstance(description, (list, tuple)):
#            description = (description,)
#            
#        cms = []
#        for d in description:
#            if isinstance(d, dict):
#                cms.append([self._CellMethod(**d)])
#            elif isinstance(d, basestring):
#                cms.append(self._CellMethod.parse(d))
#            elif isinstance(d, self._CellMethod):
#                cms.append([d])
#            else:
#                raise ValueError("asd 123948u m  BAD DESCRIPTION TYPE")
#        #--- End: for
#
#        keys = self.cell_methods().keys()                    
#        f_cell_methods = self.cell_methods().values()
#        nf = len(f_cell_methods)
#
#        out = {}
#        
#        for d in cms:
#            c = self._conform_cell_methods(d)
#
#            n = len(c)
#            for j in range(nf-n+1):
#                found_match = True
#                for i in range(0, n):
#                    if not f_cell_methods[j+i].match(c[i].properties()):
#                        found_match = False
#                        break
#                #--- End: for
#            
#                if not found_match:
#                    continue
#
#                # Still here?
#                key = tuple(keys[j:j+n])
#                if len(key) == 1:
#                    key = key[0]
#
#                if key not in out:
#                    value = f_cell_methods[j:j+n]
#                    if copy:
#                    value = [cm.copy() for cm in value]                        
#
#                out[key] = value
#            #--- End: for
#        #--- End: for
#        
#        return out
#    #--- End: def
    
#--- End: class
