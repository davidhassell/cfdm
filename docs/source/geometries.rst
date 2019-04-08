.. currentmodule:: cfdm
.. default-role:: obj

<in coordinates section>

For some geospatial applications, data values are associated with a
`geometry`_, which is a spatial representation of a real-world
feature, for instance a time-series of areal average precipitation
over a watershed. Geometries are a generalization of cell bounds and
points, lines or polygons; and they may include several disjoint
parts.

.. _Geometries:

**Geometries**
--------------

----

:ref:`Geometries <TODO>` in netCDF datasets are always compressed to
save space, but the CF data model views them in their uncompressed
form, as for other types of compressed netCDF variables.

This compression uses either continguous, indexed or indexed
contiguous ragged array techniques, although their netCDF encoding
differs compared to :ref:`discrete sampling geometries
<Discrete-sampling-geometries>`.

This is illustrated with the file ``geometry.nc`` (:download:`download
<netcdf_files/geometry.nc>`, TODO kB) [#files]_:

.. code-block:: shell
   :caption: *Inspect the geometries dataset with the ncdump command
             line tool.*
   
   $ ncdump -h geometry.nc
   TODO

Reading and inspecting this file shows the bounds data of the
latitude, longitude and altitude coordinates presented in
three-dimensional uncompressed form (one extra dimension for the
geometry parts, and one for the geometry nodes within each part), and
the interior ring data is presented in two-dimensional uncompressed
form (one extra dimension for the status of each part). In both cases,
the underlying arrays is still in the one-dimension ragged
representation described in the file:

.. code-block:: python3
   :caption: *Read a field construct from a dataset that has been
             compressed with contiguous ragged arrays, and inspect its
             data in uncompressed form.*
   
   >>> h = cfdm.read('contiguous.nc')[0]
   >>> print(h)
   Field: specific_humidity (ncvar%humidity)
   -----------------------------------------
   Data            : specific_humidity(ncdim%station(4), ncdim%timeseries(9))
   Dimension coords: 
   Auxiliary coords: time(ncdim%station(4), ncdim%timeseries(9)) = [[1969-12-29 00:00:00, ..., 1970-01-07 00:00:00]]
                   : latitude(ncdim%station(4)) = [-9.0, ..., 78.0] degrees_north
                   : longitude(ncdim%station(4)) = [-23.0, ..., 178.0] degrees_east
                   : height(ncdim%station(4)) = [0.5, ..., 345.0] m
                   : cf_role:timeseries_id(ncdim%station(4)) = [station1, ..., station4]
   >>> print(h.data.array)
   [[0.12 0.05 0.18   --   --   --   --   --   --]
    [0.05 0.11 0.2  0.15 0.08 0.04 0.06   --   --]
    [0.15 0.19 0.15 0.17 0.07   --   --   --   --]
    [0.11 0.03 0.14 0.16 0.02 0.09 0.1  0.04 0.11]]

.. code-block:: python3
   :caption: *Inspect the underlying compressed array and the count
             variable that defines how to uncompress the data.*
	     
   >>> h.data.get_compression_type()
   'ragged contiguous'
   >>> print(h.data.compressed_array)
   [0.12 0.05 0.18 0.05 0.11 0.2 0.15 0.08 0.04 0.06 0.15 0.19 0.15 0.17 0.07
    0.11 0.03 0.14 0.16 0.02 0.09 0.1 0.04 0.11]
   >>> count_variable = h.data.get_count_variable()
   >>> count_variable
   <Count: long_name=number of observations for this station(4) >
   >>> print(count_variable.data.array)
   [3 7 5 9]

The timeseries for the second station is easily selected by indexing
the "station" axis of the field construct:

.. code-block:: python3
   :caption: *Get the data for the second station.*
	  
   >>> station2 = h[1]
   >>> station2
   <Field: specific_humidity(ncdim%station(1), ncdim%timeseries(9))>
   >>> print(station2.data.array)
   [[0.05 0.11 0.2 0.15 0.08 0.04 0.06 -- --]]

The underlying array of original data remains in compressed form until
data array elements are modified:
   
.. code-block:: python3
   :caption: *Change an element of the data and show that the
             underlying array is no longer compressed.*

   >>> h.data.get_compression_type()
   'ragged contiguous'
   >>> h.data[1, 2] = -9
   >>> print(h.data.array)
   [[0.12 0.05 0.18   --   --   --   --   --   --]
    [0.05 0.11 -9.0 0.15 0.08 0.04 0.06   --   --]
    [0.15 0.19 0.15 0.17 0.07   --   --   --   --]
    [0.11 0.03 0.14 0.16 0.02 0.09 0.1  0.04 0.11]]
   >>> h.data.get_compression_type()
   ''

A construct with an underlying ragged array is created by initialising
a `Data` instance with a ragged array that is stored in one of three
special array objects: `RaggedContiguousArray`, `RaggedIndexedArray`
or `RaggedIndexedContiguousArray`. The following code creates a simple
field construct with an underlying contiguous ragged array:

.. code-block:: python3
   :caption: *Create a field construct with compressed data.*

   import numpy
   import cfdm
   
   # Define the ragged array values
   ragged_array = numpy.array([280, 282.5, 281, 279, 278, 279.5],
                              dtype='float32')

   # Define the count array values
   count_array = [2, 4]

   # Create the count variable
   count_variable = cfdm.Count(data=cfdm.Data(count_array))
   count_variable.set_property('long_name', 'number of obs for this timeseries')

   # Create the contiguous ragged array object
   array = cfdm.RaggedContiguousArray(
                    compressed_array=cfdm.NumpyArray(ragged_array),
                    shape=(2, 4), size=8, ndim=2,
                    count_variable=count_variable)

   # Create the field construct with the domain axes and the ragged
   # array
   T = cfdm.Field()
   T.set_properties({'standard_name': 'air_temperature',
                     'units': 'K',
                     'featureType': 'timeSeries'})
   
   # Create the domain axis constructs for the uncompressed array
   X = T.set_construct(cfdm.DomainAxis(4))
   Y = T.set_construct(cfdm.DomainAxis(2))
   
   # Set the data for the field
   T.set_data(cfdm.Data(array), axes=[Y, X])
				
The new field construct can now be inspected and written to a netCDF file:

.. code-block:: python3
   :caption: *Inspect the new field construct and write it to disk.*
   
   >>> T
   <Field: air_temperature(key%domainaxis1(2), key%domainaxis0(4)) K>
   >>> print(T.data.array)
   [[280.0 282.5    --    --]
    [281.0 279.0 278.0 279.5]]
   >>> T.data.get_compression_type()
   'ragged contiguous'
   >>> print(T.data.compressed_array)
   [280.  282.5 281.  279.  278.  279.5]
   >>> count_variable = T.data.get_count_variable()
   >>> count_variable
   <Count: long_name=number of obs for this timeseries(2) >
   >>> print(count_variable.data.array)
   [2 4]
   >>> cfdm.write(T, 'T_contiguous.nc')

The content of the new file is:
  
.. code-block:: shell
   :caption: *Inspect the new compressed dataset with the ncdump
             command line tool.*   

   $ ncdump T_contiguous.nc
   netcdf T_contiguous {
   dimensions:
   	dim = 2 ;
   	element = 6 ;
   variables:
   	int64 count(dim) ;
   		count:long_name = "number of obs for this timeseries" ;
   		count:sample_dimension = "element" ;
   	float air_temperature(element) ;
   		air_temperature:units = "K" ;
   		air_temperature:standard_name = "air_temperature" ;
   
   // global attributes:
		:Conventions = "CF-1.7" ;
		:featureType = "timeSeries" ;
   data:
   
    count = 2, 4 ;
   
    air_temperature = 280, 282.5, 281, 279, 278, 279.5 ;
   }
