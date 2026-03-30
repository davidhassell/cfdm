import datetime
import os
import unittest

import numpy as np
import cfdm

from cfdm.read_write.netcdf.p5netcdf import File, Dimension, Variable, Group


_format_attr = cfdm.read_write.netcdf.p5netcdf._format_attr

class TestP5NetCDF(unittest.TestCase):
    """Test suite for the p5netcdf read-only NetCDF-4 implementation."""
    
    @classmethod
    def setUpClass(cls):
        """Create a real NetCDF-4 file on disk using cfdm.example_field."""
        cls.test_file = "test_p5_example.nc"
        
        # Grab a standard example field (typically a 2D specific humidity field)
        f = cfdm.example_field(0)
        
        # Add a custom attribute to test single-byte integer decoding ('1' = 49)
        f.set_property('int32', np.int32(49))
        f.set_property('int64', np.int64(49))
        f.set_property('float32', np.float32(49.0))
        f.set_property('float64', np.float64(49.0))
        f.set_property('list', [2.5, 3.5])
        
        # Write it to disk as NetCDF-4
        cfdm.write(f, cls.test_file, fmt='NETCDF4')

#    @classmethod
#    def tearDownClass(cls):
#        """Clean up the generated test file."""
#        if os.path.exists(cls.test_file):
#            os.remove(cls.test_file)

    def setUp(self):
        """Open the file for reading before each test."""
        self.nc = File(self.test_file)

    def tearDown(self):
        """Close the file after each test."""
        self.nc.close()

    # ------------------------------------------------------------------
    # Attribute Formatter Tests
    # ------------------------------------------------------------------
    def test_format_attr_numpy_scalar(self):
        """Test that 1-element numpy arrays are flattened to scalars."""
        val = np.array([38.0])
        self.assertEqual(_format_attr(val), 38.0)
        self.assertNotIsInstance(_format_attr(val), np.ndarray)

    def test_format_attr_decoding(self):
        """Test attribute decoding."""
        attrs= self.nc['q'].attrs
        self.assertIsInstance(attrs['int32'], np.int32)
        self.assertIsInstance(attrs['int64'], np.int64)
        self.assertIsInstance(attrs['float32'], np.float32)
        self.assertIsInstance(attrs['float64'], np.float64)
        self.assertIsInstance(attrs['list'], np.ndarray)
        self.assertIsInstance(attrs['units'], str)

    # ------------------------------------------------------------------
    # File & Group Tests
    # ------------------------------------------------------------------
    def test_file_properties(self):
        """Test basic properties of the File root object."""
        self.assertEqual(self.nc.name, '/')
        self.assertEqual(self.nc.filename, self.test_file)

    def test_group_attributes(self):
        """Test that global attributes are parsed and ignored list works."""
        # Conventions is standard on cfdm example fields
        self.assertTrue(self.nc.attrs.get('Conventions').startswith('CF-'))
        
        # Check that hidden attributes didn't bleed through
        self.assertNotIn('_NCProperties', self.nc.attrs)
        self.assertNotIn('_nc3_strict', self.nc.attrs)

    def test_group_contains_variables(self):
        """Test that variables are correctly mapped into the group."""
        # The example field typically has lat, lon, and the data variable
        self.assertIn('lat', self.nc.variables)
        self.assertIn('lon', self.nc.variables)
        self.assertGreaterEqual(len(self.nc.variables), 3)

    # ------------------------------------------------------------------
    # Dimension Tests
    # ------------------------------------------------------------------
    def test_dimension_sizes(self):
        """Test that sizes are correctly read and __len__ works."""
        self.assertIn('lat', self.nc.dimensions)
        dim_lat = self.nc.dimensions['lat']
        self.assertGreater(dim_lat.size, 0)
        self.assertEqual(len(dim_lat), dim_lat.size)

    def test_dimension_isunlimited(self):
        """Test that isunlimited returns a boolean."""
        dim_lat = self.nc.dimensions['lat']
        self.assertIsInstance(dim_lat.isunlimited(), bool)

    # ------------------------------------------------------------------
    # Variable Tests
    # ------------------------------------------------------------------
    def test_variable_coordinate_dimensions(self):
        """Test that dimension scales resolve their own name as a dimension."""
        lat_var = self.nc.variables['lat']
        self.assertEqual(lat_var.dimensions, ('lat',))
        
        lon_var = self.nc.variables['lon']
        self.assertEqual(lon_var.dimensions, ('lon',))

    def test_variable_get_dims(self):
        """Test that Variable.get_dims returns actual Dimension objects."""
        lat_var = self.nc.variables['lat']
        dims = lat_var.get_dims()
        
        self.assertEqual(len(dims), 1)
        self.assertIsInstance(dims[0], Dimension)
        self.assertEqual(dims[0].name, 'lat')

    def test_variable_numpy_slicing(self):
        """Test that variable indexing correctly calls the underlying pyfive dataset."""
        lat_var = self.nc.variables['lat']
        data = lat_var[:]
        self.assertIsInstance(data, np.ndarray)
        self.assertEqual(data.shape, lat_var.shape)

    # ------------------------------------------------------------------
    # Path Routing Tests
    # ------------------------------------------------------------------
    def test_absolute_path_lookup(self):
        """Test that leading slashes are stripped and resolve perfectly."""
        lat_var_direct = self.nc.variables['lat']
        lat_var_path = self.nc['/lat']
        self.assertEqual(lat_var_direct.name, lat_var_path.name)


if __name__ == "__main__":
    print("Run date:", datetime.datetime.now())
    cfdm.environment()
    print("")
    unittest.main(verbosity=2)
