.. currentmodule:: cfdm
.. default-role:: obj

<in coordinates section>

For some geospatial applications, data values are associated with a
`geometry`_, which is a spatial representation of a real-world
feature, for instance a time-series of areal average precipitation
over a watershed. Geometries are a generalization of cell bounds that
allows for points, lines or polygons; and they may include several
disjoint parts. See the :ref:`Geometries` section for examples and
further details.

.. _Geometries:

**Geometries**
--------------

----

For some geospatial applications, data values are associated with a
`geometry`_, which is a spatial representation of a real-world
feature, for instance a time-series of areal average precipitation
over a watershed. Geometries are a generalization of cell bounds that
allows for points, lines or polygons; and they may include several
disjoint parts.

Geometries in netCDF datasets are always compressed to save space, but
the CF data model views them in their uncompressed form, as is the
case for other types of compressed netCDF variables.

This compression uses either continguous, indexed or indexed
contiguous ragged array techniques which are similar to those used for
:ref:`discrete sampling geometries <Discrete-sampling-geometries>`,
but their netCDF encoding is different.

This is illustrated with the file ``geometry.nc`` (:download:`download
<netcdf_files/geometry.nc>`, TODO kB) [#files]_:

.. code-block:: shell
   :caption: *Inspect the geometries dataset with the ncdump command
             line tool.*
   
   $ ncdump -h geometry.nc
   netcdf geometry {
   dimensions:
   	time = 4 ;
   	instance = 2 ;
   	node = 13 ;
   	part = 4 ;
   	strlen = 2 ;
   variables:
   	int time(time) ;
   		time:standard_name = "time" ;
   		time:units = "days since 2000-01-01" ;
   	char instance_id(instance, strlen) ;
   		instance_id:cf_role = "timeseries_id" ;
   	double x(node) ;
   		x:units = "degrees_east" ;
   		x:standard_name = "longitude" ;
   		x:axis = "X" ;
   	double y(node) ;
   		y:units = "degrees_north" ;
   		y:standard_name = "latitude" ;
   		y:axis = "Y" ;
   	double z(node) ;
   		z:units = "m" ;
   		z:standard_name = "altitude" ;
   		z:axis = "Z" ;
   	double lat(instance) ;
   		lat:units = "degrees_north" ;
   		lat:standard_name = "latitude" ;
   		lat:nodes = "y" ;
   	double lon(instance) ;
   		lon:units = "degrees_east" ;
   		lon:standard_name = "longitude" ;
   		lon:nodes = "x" ;
   	int geometry_container ;
   		geometry_container:geometry_type = "polygon" ;
   		geometry_container:node_count = "node_count" ;
   		geometry_container:node_coordinates = "x y z" ;
   		geometry_container:grid_mapping = "datum" ;
   		geometry_container:coordinates = "lat lon" ;
   		geometry_container:part_node_count = "part_node_count" ;
   		geometry_container:interior_ring = "interior_ring" ;
   		geometry_container:geometry_dimension = "instance" ;
   	int node_count(instance) ;
   	int part_node_count(part) ;
   	int interior_ring(part) ;
   	float datum ;
   		datum:grid_mapping_name = "latitude_longitude" ;
   		datum:semi_major_axis = 6378137. ;
   		datum:inverse_flattening = 298.257223563 ;
   		datum:longitude_of_prime_meridian = 0. ;
   	double pr(instance, time) ;
   		pr:standard_name = "preciptitation_amount" ;
   		pr:standard_units = "kg m-2" ;
   		pr:coordinates = "time lat lon instance_id" ;
   		pr:grid_mapping = "datum" ;
   		pr:geometry = "geometry_container" ;
   
   // global attributes:
   		:Conventions = "CF-1.8" ;
   }

Reading and inspecting this file shows the bounds of the coordinate
constructs presented in three-dimensional uncompressed form (one extra
dimension for the geometry parts, and one for the geometry nodes
within each part), and the interior ring data presented in
two-dimensional uncompressed form (one extra dimension for the status
of each part):

.. code-block:: python3
   :caption: *Read a field construct from a dataset that has been
             compressed with contiguous ragged arrays, and inspect its
             data in uncompressed form.*
   
   >>> pr = cfdm.read('geometry.nc')[0]
   >>> print(pr)
   Field: preciptitation_amount (ncvar%pr)
   ---------------------------------------
   Data            : preciptitation_amount(cf_role=timeseries_id(2), time(4))
   Dimension coords: time(4) = [2000-01-02 00:00:00, ..., 2000-01-05 00:00:00]
   Auxiliary coords: latitude(cf_role=timeseries_id(2)) = [25.0, 7.0] degrees_north
                   : longitude(cf_role=timeseries_id(2)) = [10.0, 40.0] degrees_east
                   : cf_role=timeseries_id(cf_role=timeseries_id(2)) = [x1, y2]
                   : altitude(cf_role=timeseries_id(2), 3, 4) = [[[1.0, ..., --]]] m
   Coord references: grid_mapping_name:latitude_longitude
   >>> z = h.construct('altitude')
   >>> z.dump()
   Auxiliary coordinate: altitude
       Geometry: polygon
       Bounds:axis = 'Z'
       Bounds:standard_name = 'altitude'
       Bounds:units = 'm'
       Bounds:Data(2, 3, 4) = [[[1.0, ..., --]]] m       
       Interior Ring:Data(2, 3) = [[0, ..., --]]
   >>> print(z.bounds.data.array)
   [[[1.0 2.0 4.0 --]
     [2.0 3.0 4.0 5.0]
     [5.0 1.0 4.0 --]]
   
    [[3.0 2.0 1.0 --]
     [-- -- -- --]
     [-- -- -- --]]]
   >>> print(z.interior_ring.data.array)
   [[0 1 0]
    [0 -- --]]

Geometry node (and interior ring) data values are accessed, and may be
altered, by indexing the uncompressed dimensions:

.. code-block:: python3
   :caption: *TODO*
	     
   >>> z.bounds.data[0, 2, 3]
   <Data(1, 1, 1): [[[5.0]]] m>
   >>> z.bounds.data[0, 2, 3] = 99
   >>> print(z.bounds.data.array)
   [[[1.0 2.0 4.0 --]
     [2.0 3.0 4.0 99.0]
     [5.0 1.0 4.0 --]]
   
    [[3.0 2.0 1.0 --]
     [-- -- -- --]
     [-- -- -- --]]]

Geometry nodes are always written to netCDF datasets in their
compressed form, and the compression is applied automatically. In
particular, the node count and part node count data are calculated and
are not be set manually. It is possible, however, to provide
properties to node count, part node count and interior ring variables
that will be written as netCDF attributes to the corresponding netCDF
variables.
     
.. code-block:: python3
   :caption: *TODO*
	     
   >>> ir = z.get_interior_ring()
   >>> ir.set_property('long_name', 'Interior ring designations')
   >>> nc = z.get_node_count()
   >>> nc.set_property('long_name', 'Node counts')
   >>> pnc = z.get_part_node_count()
   >>> pnc.set_property('long_name', 'Part node counts')
   >>> pnc.nc_set_variable('pnc')
   >>> pnc.nc_set_dimension('parts')
   >>> cfdm.write(pr, 'new_pr.nc')
