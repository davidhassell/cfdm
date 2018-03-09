import abc

from copy import deepcopy

#from ..collection import Collection

# ====================================================================
#

#
# ====================================================================

class Properties(object):
    '''

Base class for storing a data array with metadata.

A variable contains a data array and metadata comprising properties to
describe the physical nature of the data.

All components of a variable are optional.

'''
    __metaclass__ = abc.ABCMeta

    _special_properties = ()

    def __init__(self, properties=None, source=None, copy=True):
        '''**Initialization**

:Parameters:

    properties: `dict`, optional

    source: optional

    copy: `bool`, optional

        '''
        self._components = {
            # Components that are considered for equality and are deep
            # copied with, both with bespoke algorithms. DO NOT ADD
            # COMPONENTS TO THIS PARTITION.
            1: {'properties': {}},
            # Components that are considered for equality via their
            # `equals` method and are deep copied with via their
            # `copy` method
            2: {},
            # Components that are *not* considered for equality but are
            # deep copied
            3: {},
            # Components that are *not* considered for equality and are
            # *not* deep copied
            4: {},
        }
 
        if source is not None:
            p = source.properties(copy=False)
            if properties:
                p.update(properties)

            properties = p

            components = source._components[2]
            if components:            
                components = components2.copy()
                if copy:
                    for key, value in components.items():
                        self._components[key] = value.copy()

                self._components[2] = components
            #--- End: if
            
            components = source._components[3]
            if components:
                components = components.copy()
                if copy:
                    for key, value in components.items():
                        components[key] = deepcopy(value)
                        
                self._components[3] = components
            #--- End: if

            components = source._components[4]
            if components:
                self._components[4] = components.copy()
        #--- End: if

        if properties:
            properties = properties.copy()
            if copy:
                for key, value in properties.items():
                    properties[key] = deepcopy(value)
                    
            self._components[1]['properties'] = properties
    #--- End: def
        
    def __deepcopy__(self, memo):
        '''Called by the `copy.deepcopy` standard library function.

.. versionadded:: 1.6

        '''
        return self.copy()
    #--- End: def

    def __repr__(self):
        '''x.__repr__() <==> repr(x)

.. versionadded:: 1.6

        '''
        return '<{0}: {1}>'.format(self.__class__.__name__, str(self))
    #--- End: def

    def _del_component(self, component_type, component, key=None):
        '''
        '''
        components = self._components[component_type]
        if key is None:            
            return components.pop(component, None)
        else:
            return components[component].pop(key, None)
    #--- End: def

    def _get_component(self, component_type, component, key, *default):
        '''
        '''
        components = self._components[component_type]
        if key is None:
            value = components.get(component)
        else:
            value = components[component].get(key)
        
        if value is None:
            if default:
                return default[0]           
            raise AttributeError("Can't get non-existent {0} {1!r} ".format(component, key))

        return value
    #--- End: def

    def _has_component(self, component_type, component, key=None):
        '''
        '''
        components = self._components[component_type]
        if key is None:
            return component in components
        else:
            return key in components[component]
    #--- End: def

    def _set_component(self, component_type, component, key, value):
        '''
        '''
        components = self._components[component_type]
        if key is None:
            components[component] = value
        else:
            components[component][key] = value
    #--- End: def

    def copy(self, data=True):
        '''Return a deep copy.

``x.copy()`` is equivalent to ``copy.deepcopy(x)``.

.. versionadded:: 1.6

:Examples 1:

>>> d = c.copy()

:Parameters:

    data: `bool`, optional
        This parameter has no effect and is ignored.

:Returns:

    out:
        The deep copy.

        '''        
        return type(self)(source=self, copy=True)
    #--- End: def
    
    def del_property(self, prop):
        '''Delete a property.

A property describes an aspect of the construct that is independent of
the domain.

A property may have any name and any value. Some properties correspond
to some netCDF attributes of variables (e.g. "units", "long_name", and
"standard_name"), or netCDF global file attributes (e.g. "history" and
"institution"),

.. versionadded:: 1.6

.. seealso:: `get_property`, `has_property`, `set_property`

:Examples 1:

>>> f.del_property('standard_name')

:Parameters:

    prop: `str`
        The name of the property to be deleted.

:Returns:

     out:
        The value of the deleted property, or `None` if the property
        was not set.

:Examples 2:

>>> f.set_property('project', 'CMIP7')
>>> print f.del_property('project')
'CMIP7'
>>> print f.del_property('project')
None

        '''
        return self._del_component(1, 'properties', prop)
    #--- End: def

    def get_property(self, prop, *default):
        '''Get a property.

A property describes an aspect of the construct that is independent of
the domain.

A property may have any name and any value. Some properties correspond to
some netCDF attributes of variables (e.g. "units", "long_name", and
"standard_name"), or netCDF global file attributes (e.g. "history" and
"institution"),

.. versionadded:: 1.6

.. seealso:: `del_property`, `has_property`, `set_property`

:Examples 1:

>>> x = f.get_property('standard_name')

:Parameters:

    prop: `str`
        The name of the property to be retrieved.

    default: optional
        Return *default* if and only if the property has not been set.

:Returns:

    out:
        The value of the property or the default value. If the
        property has not been set, then return *default* if provided
        or else raise an `ArtributeError`.

:Examples 2:

>>> f.set_property('standard_name', 'air_temperature')
>>> print f.get_property('standard_name')
'air_temperature'
>>> f.del_property('standard_name')
>>> print f.get_property('standard_name')
AttributeError: Field doesn't have property 'standard_name'
>>> print f.get_property('standard_name', 'UNSET')
'UNSET'

        '''
        try:
            return self._get_component(1, 'properties', prop, *default)
        except AttributeError:
            raise AttributeError("Can't get non-existent property {!r}".format(prop))
    #--- End: def

    def has_property(self, prop):
        '''Return True if a property has been set.

A property describes an aspect of the construct that is independent of
the domain.

A property may have any name and any value. Some properties correspond
to some netCDF attributes of variables (e.g. "units", "long_name", and
"standard_name"), or netCDF global file attributes (e.g. "history" and
"institution"),

.. versionadded:: 1.6

.. seealso:: `del_property`, `get_property`, `set_property`

:Examples 1:

>>> x = f.has_property('long_name')

:Parameters:

    prop: `str`
        The name of the property.

:Returns:

     out: `bool`
        True if the property has been set, otherwise False.

:Examples 1:

>>> if f.has_property('standard_name'):
...     print 'Has standard name'



'''
        return self._has_component(1, 'properties', prop)
    #--- End: def

    def properties(self, props=None, clear=False, copy=True):
        '''Inspect or change the CF properties.

.. versionadded:: 1.6

:Examples 1:

>>> f.{+name}()

:Parameters:

    props: `dict`, optional   
        Set {+variable} attributes from the dictionary of values. If
        the *copy* parameter is True then the values in the *attrs*
        dictionary are deep copied

    clear: `bool`, optional
        If True then delete all CF properties.

    copy: `bool`, optional
        If False then any property values provided bythe *props*
        parameter are not copied before insertion into the
        {+variable}. By default they are deep copied.

:Returns:

    out: `dict`
        The CF properties prior to being changed, or the current CF
        properties if no changes were specified.

:Examples 2:

        '''
        existing_properties = self._get_component(1, 'properties', None, None)

        if existing_properties is None:
           existing_properties = {}
           self._set_component(1, 'properties', None, existing_properties)
        
        out = existing_properties.copy()

        for prop in self._special_properties:
            value = getattr(self, 'get_'+prop)(None)
            if value is not None:
                out[prop] = value
        
        if clear:
            existing_properties.clear()
            for prop in self._special_properties:
                getattr(self, 'del_'+prop)

        if not props:
            return out

        # Still here?
        if copy:
            props = deepcopy(props)

        existing_properties.update(props)

        return out
    #--- End: def

    def set_property(self, prop, value):
        '''Set a property.

A property describes an aspect of the construct that is independent of
the domain.

A property may have any name and any value. Some properties correspond
to some netCDF attributes of variables (e.g. "units", "long_name", and
"standard_name"), or netCDF global file attributes (e.g. "history" and
"institution"),

.. versionadded:: 1.6

.. seealso:: `del_property`, `get_property`, `has_property`

:Examples 1:

>>> f.set_property('standard_name', 'time')

:Parameters:

    prop: `str`
        The name of the property.

    value:
        The value for the property.

:Returns:

     `None`

        '''
        self._set_component(1, 'properties', prop, value)
    #--- End: def

#--- End: class