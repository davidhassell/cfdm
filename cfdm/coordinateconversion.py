from . import mixin
from . import core


class CoordinateConversion(mixin.ParametersDomainAncillaries,
                           core.CoordinateConversion):
    '''A coordinate conversion formula of a coordinate reference construct
of the CF data model.

A coordinate conversion formula converting coordinate values taken
from the dimension or auxiliary coordinate constructs to a different
coordinate system. A term of the conversion formula can be a scalar or
vector parameter which does not depend on any domain axis constructs,
may have units, or may be a descriptive string (such as the projection
name "mercator"), or it can be a reference to a domain ancillary
construct (such as one containing spatially varying orography data). A
coordinate reference construct relates the coordinate values of the
field to locations in a planetary reference frame.

    '''
    
    # Ancillary-valued terms are stored as references to external
    # objects
    _internal_ancillaries = False

#--- End: class
