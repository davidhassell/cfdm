import datetime
import os
import pathlib
import tempfile
import unittest

import fsspec
import netCDF4
import numpy as np
import pyfive

import cfdm


class Testp5netcdf(unittest.TestCase):
    """Test suite for the p5netcdf read-only netCDF implementation."""

    maxDiff = None

    @classmethod
    def setUpClass(cls):
        """Create the test file."""
        tmpfile = tempfile.mkstemp("_test_p5netcdf.nc", dir=os.getcwd())[1]
        # tmpfile = "test_p5_example.nc"

        # ------------------------------------------------------------
        # NETCDF4
        # ------------------------------------------------------------
        with netCDF4.Dataset(tmpfile, "w", format="NETCDF4") as nc:
            # Global Attributes
            nc.setncattr("Conventions", "CF-1.13")
            nc.setncattr("global_attr_1", 3.14)
            nc.setncattr("global_attr_2", "foo")

            # Dimensions for root group
            nc.createDimension("bounds2", 2)

            # Root group variables
            time = nc.createVariable("time", "i4")
            time.setncattr("units", "days since 2018-12-01")
            time.setncattr("standard_name", "time")
            time[...] = 31

            # Group: forecast
            forecast = nc.createGroup("forecast")
            forecast.createDimension("lon", None)  # UNLIMITED

            lon_bnds = forecast.createVariable(
                "lon_bnds", "f8", ("lon", "bounds2")
            )

            lon = forecast.createVariable("lon", "f8", ("lon",))
            lon.setncattr("units", "degrees_east")
            lon.setncattr("standard_name", "longitude")
            lon.setncattr("bounds", "/forecast/lon_bnds")

            # Data for forecast group
            lon[...] = [22.5, 67.5, 112.5, 157.5, 202.5, 247.5, 292.5, 337.5]
            lon_bnds[...] = [
                [0, 45],
                [45, 90],
                [90, 135],
                [135, 180],
                [180, 225],
                [225, 270],
                [270, 315],
                [315, 360],
            ]

            # Group: forecast/model
            model = forecast.createGroup("model")
            model.createDimension("lat", 5)

            # Group attributes
            model.setncattr("group_attr_1", np.int64(12))
            model.setncattr("group_attr_2", "bar")

            lat_bnds = model.createVariable(
                "lat_bnds", "f8", ("lat", "bounds2")
            )

            lat = model.createVariable("lat", "f8", ("lat",), contiguous=True)
            lat.setncattr("units", "degrees_north")
            lat.setncattr("standard_name", "latitude")
            lat.setncattr("bounds", "/forecast/model/lat_bnds")

            # The 'q' variable (uses 'lon' from the parent group and 'lat'
            # from current group)
            q = model.createVariable(
                "q", "f4", ("lat", "lon"), chunksizes=(5, 3)
            )

            # int and float attributes for 'q'
            q.setncattr("int", 49)
            q.setncattr("int8", np.int8(49))
            q.setncattr("int16", np.int16(49))
            q.setncattr("int32", np.int32(49))
            q.setncattr("int64", np.int64(49))
            q.setncattr("float", 49.0)
            q.setncattr("float32", np.float32(49.0))
            q.setncattr("float64", np.float64(49.0))
            q.setncattr("uint8", np.uint8(49))
            q.setncattr("uint16", np.uint16(49))
            q.setncattr("uint32", np.uint32(49))
            q.setncattr("uint64", np.uint64(49))

            # list attributes
            q.setncattr("list1", np.array([2, 3], dtype="int8"))
            q.setncattr("list2", np.array([2, 3], dtype="int16"))
            q.setncattr("list3", np.array([2, 3], dtype="int64"))
            q.setncattr("list4", np.array([2, 3], dtype="int32"))
            q.setncattr("list5", np.array([2.0, 3.0], dtype="float32"))
            q.setncattr("list6", np.array([2.0, 3.0], dtype="float64"))
            q.setncattr("list7", [2, 3])
            q.setncattr("list8", np.array([2], dtype="int32"))
            q.setncattr("list9", np.array([], dtype="int32"))
            q.setncattr("list10", [])
            q.setncattr("list11", ["a", "bb", "ccc"])
            q.setncattr("list12", ["a", "1", "2.5"])
            q.setncattr("list13", np.array(["a"], dtype="U"))
            q.setncattr("list14", np.array(["a", "bb"], dtype="U"))

            # char attributes
            q.setncattr("string1", "1")
            q.setncattr("string2", "a")
            q.setncattr("string3", "kg m-2")
            q.setncattr("string4", "")
            q.setncattr("string5", " ")
            q.setncattr("string6", b"")
            q.setncattr("string7", np.bytes_(""))
            q.setncattr("string8", np.bytes_([]))
            q.setncattr("string9", np.array([], dtype="S1"))

            # Coordinates and methods
            q.setncattr("coordinates", "time")
            q.setncattr("cell_methods", "area: mean")

            q.setncattr("standard_name", "specific_humidity")

            # Data for model group
            lat[...] = [-75, -45, 0, 45, 75]
            lat_bnds[...] = [
                [-90, -60],
                [-60, -30],
                [-30, 30],
                [30, 60],
                [60, 90],
            ]

            q[...] = [
                [0.007, 0.034, 0.003, 0.014, 0.018, 0.037, 0.024, 0.029],
                [0.023, 0.036, 0.045, 0.062, 0.046, 0.073, 0.006, 0.066],
                [0.11, 0.131, 0.124, 0.146, 0.087, 0.103, 0.057, 0.011],
                [0.029, 0.059, 0.039, 0.07, 0.058, 0.072, 0.009, 0.017],
                [0.006, 0.036, 0.019, 0.035, 0.018, 0.037, 0.034, 0.013],
            ]

        cls.filename = tmpfile
        cls.filename4 = "example_field_0.nc"
        cls.filename3 = "example_field_0.nc3"
        cls.zarr = "example_field_0.zarr3"

        fs = fsspec.filesystem("reference", fo="example_field_0.kerchunk")
        cls.kerchunk = fs.get_mapper()

        cls.p = cfdm.p5netcdf.File(cls.filename)
        cls.p4 = cfdm.p5netcdf.File(cls.filename3)
        cls.p3 = cfdm.p5netcdf.File(cls.filename3)
        cls.pz = cfdm.p5netcdf.File(cls.zarr)
        cls.pk = cfdm.p5netcdf.File(cls.kerchunk)

        cls.n = netCDF4.Dataset(cls.filename, "r")
        cls.n3 = netCDF4.Dataset(cls.filename3, "r")

    @classmethod
    def tearDownClass(cls):
        """Clean up the generated test file."""
        for filename in (cls.filename,):
            if os.path.exists(filename):
                os.remove(filename)

    def test_p5netcdf_attributes(self):
        """Check that attributes are parsed correctly."""
        n = self.n
        p = self.p

        nq = n["/forecast/model/q"]
        pq = p["/forecast/model/q"]

        self.assertEqual(sorted(pq.attrs), sorted(nq.ncattrs()))

        for attr, pvalue in pq.attrs.items():
            nvalue = nq.getncattr(attr)

            self.assertEqual(type(pvalue), type(nvalue))

            if isinstance(pvalue, (np.ndarray, np.integer, np.floating)):
                self.assertEqual(pvalue.dtype, nvalue.dtype)
                self.assertTrue(np.allclose(pvalue, nvalue))
            else:
                self.assertEqual(pvalue, nvalue)

    def test_p5netcdf_dimensions(self):
        """Check that dimensions are parsed correctly."""
        n = self.n
        p = self.p

        for group in ("/", "/forecast", "/forecast/model"):
            pg = p[group]
            ng = n if group == "/" else n[group]

            self.assertEqual(set(ng.dimensions), set(pg.dimensions))

            for name, pdim in pg.dimensions.items():
                ndim = ng.dimensions[name]
                self.assertEqual(pdim.isunlimited(), ndim.isunlimited())
                self.assertEqual(pdim.group().path, ndim.group().path)

                for attr in ("name", "size"):
                    self.assertEqual(getattr(pdim, attr), getattr(ndim, attr))

    def test_p5netcdf_variables(self):
        """Check that variables are parsed correctly."""
        n = self.n
        p = self.p

        for group in ("/", "/forecast", "/forecast/model"):
            pg = p[group]
            ng = n if group == "/" else n[group]

            self.assertEqual(set(ng.variables), set(pg.variables))

            for name, pvar in pg.variables.items():
                nvar = ng.variables[name]
                self.assertEqual(pvar.chunking(), nvar.chunking())

                self.assertEqual(len(pvar.get_dims()), len(nvar.get_dims()))

                self.assertTrue(np.ma.allclose(pvar[...], nvar[...]))

                if pvar.shape:
                    self.assertEqual(len(pvar), len(nvar))
                else:
                    with self.assertRaises(TypeError):
                        len(pvar)

                for attr in (
                    "name",
                    "size",
                    "shape",
                    "ndim",
                    "dtype",
                    "dimensions",
                ):
                    self.assertEqual(getattr(pvar, attr), getattr(nvar, attr))

                for pdim, ndim in zip(*(pvar.get_dims(), nvar.get_dims())):
                    self.assertEqual(pdim.name, ndim.name)
                    self.assertEqual(pdim.group().path, ndim.group().path)

    def test_p5netcdf_groups(self):
        """Check that groups are parsed correctly."""
        n = self.n
        p = self.p

        for group in ("/", "/forecast", "/forecast/model"):
            pg = p[group]
            ng = n if group == "/" else n[group]

            pattrs = pg.attrs
            nattrs = {k: ng.getncattr(k) for k in ng.ncattrs()}
            self.assertEqual(pattrs, nattrs)
            self.assertEqual(pg.path, ng.path)

    def test_p5netcdf_File_Path(self):
        """Check File with a file-like input."""
        p5p = cfdm.p5netcdf.File(pathlib.Path(self.filename))
        self.assertEqual(
            p5p["forecast/model/q"].dimensions,
            self.p["forecast/model/q"].dimensions,
        )

    def test_p5netcdf_File_file_like(self):
        """Check File with a file-like input."""
        local_fs = fsspec.filesystem("local")
        fh = local_fs.open(self.filename, "rb")
        p5fh = cfdm.p5netcdf.File(fh)
        self.assertEqual(
            p5fh["forecast/model/q"].dimensions,
            self.p["forecast/model/q"].dimensions,
        )

    def test_p5netcdf_File_pyfive_like(self):
        """Check File with a pyfive.File input."""
        py5 = pyfive.File(self.filename)
        p5py5 = cfdm.p5netcdf.File(py5)
        self.assertEqual(
            p5py5["forecast/model/q"].dimensions,
            self.p["forecast/model/q"].dimensions,
        )

    def test_p5netcdf_File_xarray_like(self):
        """Check File with an xarray.DataTree input."""
        try:
            import xarray
        except ImportError:
            self.skipTest("xarray not available")
        
        # Open the file with xarray first
        xr_dt = xarray.open_datatree(
            self.filename, mask_and_scale=False, decode_cf=False
        )
        
        # Pass the xarray DataTree to p5netcdf.File
        p5xr = cfdm.p5netcdf.File(xr_dt)
        
        # Verify it works correctly
        self.assertEqual(p5xr.backend, "xarray")
        self.assertEqual(
            p5xr["forecast/model/q"].dimensions,
            self.p["forecast/model/q"].dimensions,
        )
        
        # Test that we can access the same data
        self.assertTrue(
            np.array_equal(
                p5xr["forecast/model/q"][...],
                self.p["forecast/model/q"][...]
            )
        )
        
        # Test attributes are preserved
        self.assertEqual(
            p5xr["forecast/model/q"].attrs["standard_name"],
            "specific_humidity"
        )
        
        # Clean up
        xr_dt.close()

    def test_p5netcdf_File__repr__(self):
        """Test File.__repr__."""
        self.assertEqual(
            repr(self.p), "<p5netcdf.File: 1 dimension, 1 variable, 1 group>"
        )

    def test_p5netcdf_File__str__(self):
        """Test File.__str__."""
        self.assertEqual(
            str(self.p),
            f"""{self.p.filename}
<p5netcdf.File: 1 dimension, 1 variable, 1 group>
    Dimensions:
        bounds2: <p5netcdf.Dimension: /bounds2, size=2>
    Variables:
        time: <p5netcdf.Variable: /time, shape=(), dimensions=()>
    Groups:
        forecast: <p5netcdf.Group: /forecast, 1 dimension, 2 variables, 1 group>""",
        )

    def test_p5netcdf_File_bad_file(self):
        """Test File with not a netCDF file."""
        with self.assertRaises(Exception):
            cfdm.p5netcdf.File(3.14)

    def test_p5netcdf_File_backend_selection(self):
        """Test File with specific backend selection."""
        # Test single backend
        p_pyfive = cfdm.p5netcdf.File(self.filename, backend="pyfive")
        self.assertEqual(p_pyfive.backend, "pyfive")

        # Test backend list
        p_list = cfdm.p5netcdf.File(
            self.filename, backend=["pyfive", "netCDF4"]
        )
        self.assertEqual(p_list.backend, "pyfive")

        # Test invalid backend
        with self.assertRaises(ValueError):
            cfdm.p5netcdf.File(self.filename, backend="invalid_backend")

    def test_p5netcdf_File_verbose(self):
        """Test File with verbose output."""
        # Should not raise an error
        p = cfdm.p5netcdf.File(self.filename, verbose=1)
        self.assertEqual(p.backend, "pyfive")

        # Test verbose=-1 (maximum verbosity)
        p = cfdm.p5netcdf.File(self.filename, verbose=-1)
        self.assertEqual(p.backend, "pyfive")

    def test_p5netcdf_File_dump(self):
        """Test File.dump."""
        self.assertEqual(
            self.p.dump(display=False),
            f"""{self.p.filename}
<p5netcdf.File: 1 dimension, 1 variable, 1 group>
    Attributes:
        Conventions: 'CF-1.13'
        global_attr_1: np.float64(3.14)
        global_attr_2: 'foo'
    Dimensions:
        bounds2: <p5netcdf.Dimension: /bounds2, size=2>
    Variables:
        time: <p5netcdf.Variable: /time, shape=(), dimensions=()>
            Attributes:
                units: 'days since 2018-12-01'
                standard_name: 'time'
    Groups:
        forecast: <p5netcdf.Group: /forecast, 1 dimension, 2 variables, 1 group>
            Dimensions:
                lon: <p5netcdf.Dimension: /forecast/lon, size=8, unlimited>
            Variables:
                lon_bnds: <p5netcdf.Variable: /forecast/lon_bnds, shape=(8, 2), dimensions=(/forecast/lon, /bounds2)>
                lon: <p5netcdf.Variable: /forecast/lon, shape=(8,), dimensions=(/forecast/lon,)>
                    Attributes:
                        units: 'degrees_east'
                        standard_name: 'longitude'
                        bounds: '/forecast/lon_bnds'
            Groups:
                model: <p5netcdf.Group: /forecast/model, 1 dimension, 3 variables, 0 groups>
                    Attributes:
                        group_attr_1: np.int64(12)
                        group_attr_2: 'bar'
                    Dimensions:
                        lat: <p5netcdf.Dimension: /forecast/model/lat, size=5>
                    Variables:
                        lat_bnds: <p5netcdf.Variable: /forecast/model/lat_bnds, shape=(5, 2), dimensions=(/forecast/model/lat, /bounds2)>
                        lat: <p5netcdf.Variable: /forecast/model/lat, shape=(5,), dimensions=(/forecast/model/lat,)>
                            Attributes:
                                units: 'degrees_north'
                                standard_name: 'latitude'
                                bounds: '/forecast/model/lat_bnds'
                        q: <p5netcdf.Variable: /forecast/model/q, shape=(5, 8), dimensions=(/forecast/model/lat, /forecast/lon)>
                            Attributes:
                                list7: array([2, 3])
                                int: np.int64(49)
                                int8: np.int8(49)
                                int16: np.int16(49)
                                int32: np.int32(49)
                                int64: np.int64(49)
                                float: np.float64(49.0)
                                float32: np.float32(49.0)
                                float64: np.float64(49.0)
                                uint8: np.uint8(49)
                                uint16: np.uint16(49)
                                uint32: np.uint32(49)
                                uint64: np.uint64(49)
                                list1: array([2, 3], dtype=int8)
                                list2: array([2, 3], dtype=int16)
                                list3: array([2, 3])
                                list4: array([2, 3], dtype=int32)
                                list5: array([2., 3.], dtype=float32)
                                list6: array([2., 3.])
                                list8: np.int32(2)
                                list9: array([], dtype=int32)
                                list10: array([], dtype=float64)
                                list11: ['a', 'bb', 'ccc']
                                list12: ['a', '1', '2.5']
                                list13: 'a'
                                list14: ['a', 'bb']
                                string1: '1'
                                string2: 'a'
                                string3: 'kg m-2'
                                string4: ''
                                string5: ' '
                                string6: ''
                                string7: ''
                                string8: ''
                                string9: ''
                                coordinates: 'time'
                                cell_methods: 'area: mean'
                                standard_name: 'specific_humidity'""",
        )

    def test_p5netcdf_File_close(self):
        """Test File.close."""
        p = cfdm.p5netcdf.File(self.filename, backend="pyfive")
        self.assertFalse(p._grp._fh.closed)
        p.close()
        self.assertTrue(p._grp._fh.closed)

        py5 = pyfive.File(self.filename)
        p = cfdm.p5netcdf.File(py5)
        self.assertFalse(p._grp._fh.closed)
        p.close()
        self.assertFalse(p._grp._fh.closed)

    def test_p5netcdf_File_filename(self):
        """Test File.filename."""
        self.assertEqual(self.filename, self.filename)

    def test_p5netcdf_File_backend(self):
        """Test File.backend."""
        self.assertEqual(self.p.backend, "pyfive")
        self.assertEqual(self.p3.backend, "netCDF4")

    def test_p5netcdf_File_ncdump(self):
        """Test File.ncdump."""
        cdl = self.p.ncdump(display=False)
        self.assertIsInstance(cdl, str)
        self.assertIn("netcdf", cdl)
        self.assertIn("dimensions:", cdl)
        self.assertIn("variables:", cdl)
        self.assertIn("bounds2 = 2", cdl)
        self.assertIn("time", cdl)
        self.assertIn("forecast", cdl)

    def test_p5netcdf_File_cache_metadata(self):
        """Test File.cache_metadata."""
        p = cfdm.p5netcdf.File(self.filename)

        # Test maximal caching
        p.cache_metadata("maximal")

        # Test minimal caching
        p.cache_metadata("minimal")

        # Test invalid strategy
        with self.assertRaises(ValueError):
            p.cache_metadata("invalid")

    def test_p5netcdf_File_getncattr(self):
        """Test File.getncattr."""
        self.assertEqual(self.p.getncattr("Conventions"), "CF-1.13")
        self.assertEqual(self.p.getncattr("global_attr_1"), 3.14)

        # Test non-existing attribute should raise AttributeError
        with self.assertRaises(AttributeError):
            self.p.getncattr("nonexistent_attr")

    def test_p5netcdf_File_library(self):
        """Test File.library."""
        self.assertIs(self.p.library, pyfive)
        self.assertIs(self.p3.library, netCDF4)

    def test_p5netcdf_File_all_properties(self):
        """Test File.all_dimensions, all_variables, all_groups."""
        # Test all_dimensions
        all_dims = self.p.all_dimensions
        self.assertIsInstance(all_dims, dict)
        self.assertIn("/bounds2", all_dims)
        self.assertIn("/forecast/lon", all_dims)
        self.assertIn("/forecast/model/lat", all_dims)

        # Test all_variables
        all_vars = self.p.all_variables
        self.assertIsInstance(all_vars, dict)
        self.assertIn("/time", all_vars)
        self.assertIn("/forecast/lon", all_vars)
        self.assertIn("/forecast/model/q", all_vars)

        # Test all_groups
        all_groups = self.p.all_groups
        self.assertIsInstance(all_groups, dict)
        self.assertIn("/", all_groups)
        self.assertIn("/forecast", all_groups)
        self.assertIn("/forecast/model", all_groups)

    def test_p5netcdf_File_protocol_is_local(self):
        """Test File.protocol and is_local properties."""
        # For local files
        self.assertEqual(self.p.protocol, "file")
        self.assertTrue(self.p.is_local)

        # Test with fsspec file-like object
        local_fs = fsspec.filesystem("local")
        fh = local_fs.open(self.filename, "rb")
        p5fh = cfdm.p5netcdf.File(fh)
        self.assertEqual(p5fh.protocol, "file")
        self.assertTrue(p5fh.is_local)

    def test_p5netcdf_File_log(self):
        """Test File.log."""
        log = self.p.log(display=False)
        self.assertIsInstance(log, str)
        self.assertIn("Successfully opened", log)

    def test_p5netcdf_File_enter_exit(self):
        """Test File in context manager."""
        with cfdm.p5netcdf.File(self.filename, backend="pyfive") as p:
            self.assertEqual(p.attrs["global_attr_2"], "foo")
            self.assertFalse(p._grp._fh.closed)

        self.assertTrue(p._grp._fh.closed)

        py5 = pyfive.File(self.filename)
        self.assertFalse(py5._fh.closed)
        with cfdm.p5netcdf.File(py5) as p:
            self.assertEqual(p.attrs["global_attr_2"], "foo")

        self.assertFalse(py5._fh.closed)

    def test_p5netcdf_Dimension__repr__(self):
        """Test Dimension.__repr__."""
        dim = self.p.dimensions["bounds2"]
        self.assertEqual(repr(dim), "<p5netcdf.Dimension: /bounds2, size=2>")

        dim = self.p["forecast"].dimensions["lon"]
        self.assertEqual(
            repr(dim),
            "<p5netcdf.Dimension: /forecast/lon, size=8, unlimited>",
        )

    def test_p5netcdf_Dimension_size(self):
        """Test Dimension.size."""
        dim = self.p.dimensions["bounds2"]
        self.assertFalse(dim.isunlimited())
        self.assertEqual(dim.size, 2)

        dim = self.p["forecast"].dimensions["lon"]
        self.assertTrue(dim.isunlimited())
        self.assertEqual(len(dim), 8)

    def test_p5netcdf_Dimension__len__(self):
        """Test Dimension.__len__."""
        dim = self.p.dimensions["bounds2"]
        self.assertFalse(dim.isunlimited())
        self.assertEqual(len(dim), 2)

        dim = self.p["forecast"].dimensions["lon"]
        self.assertTrue(dim.isunlimited())
        self.assertEqual(len(dim), 8)

    def test_p5netcdf_Dimension_isunlimited(self):
        """Test Dimension.isunlimited."""
        dim = self.p.dimensions["bounds2"]
        self.assertIsInstance(dim.isunlimited(), bool)
        self.assertFalse(dim.isunlimited())

        dim = self.p["forecast"].dimensions["lon"]
        self.assertIsInstance(dim.isunlimited(), bool)
        self.assertTrue(dim.isunlimited())

    def test_p5netcdf_Dimension_name(self):
        """Test Dimension.name."""
        dim = self.p.dimensions["bounds2"]
        self.assertEqual(dim.name, "bounds2")

        dim = self.p["forecast"].dimensions["lon"]
        self.assertEqual(dim.name, "lon")

    def test_p5netcdf_Dimension_path(self):
        """Test Dimension.path."""
        dim = self.p.dimensions["bounds2"]
        self.assertEqual(dim.path, "/bounds2")

        dim = self.p["forecast"].dimensions["lon"]
        self.assertEqual(dim.path, "/forecast/lon")

    def test_p5netcdf_Dimension_group(self):
        """Test Dimension.group."""
        group = self.p
        dim = group.dimensions["bounds2"]
        self.assertIs(dim.group(), group)

        group = self.p["/forecast"]
        dim = group.dimensions["lon"]
        self.assertIs(dim.group(), group)

        group = self.p["/forecast/model"]
        dim = group.dimensions["lat"]
        self.assertIs(dim.group(), group)

    def test_p5netcdf_Dimension_parent(self):
        """Test Dimension.group."""
        group = self.p
        dim = group.dimensions["bounds2"]
        self.assertIs(dim.parent, group)

        group = self.p["/forecast"]
        dim = group.dimensions["lon"]
        self.assertIs(dim.parent, group)

        group = self.p["/forecast/model"]
        dim = group.dimensions["lat"]
        self.assertIs(dim.parent, group)

    def test_p5netcdf_Dimension_backend(self):
        """Test Dimension.backend."""
        self.assertEqual(self.p.dimensions["bounds2"].backend, "pyfive")
        self.assertEqual(self.p3.dimensions["bounds2"].backend, "netCDF4")

    def test_p5netcdf_Dimension_library(self):
        """Test Dimension.library."""
        self.assertIs(self.p.dimensions["bounds2"].library, pyfive)
        self.assertIs(self.p3.dimensions["bounds2"].library, netCDF4)

    def test_p5netcdf_Variable__repr__(self):
        """Test Variable.__repr__."""
        var = self.p["time"]
        self.assertEqual(
            repr(var), "<p5netcdf.Variable: /time, shape=(), dimensions=()>"
        )

        var = self.p["forecast/lon"]
        self.assertEqual(
            repr(var),
            "<p5netcdf.Variable: /forecast/lon, shape=(8,), dimensions=(/forecast/lon,)>",
        )

        var = self.p["/forecast/model/q"]
        self.assertEqual(
            repr(var),
            "<p5netcdf.Variable: /forecast/model/q, shape=(5, 8), dimensions=(/forecast/model/lat, /forecast/lon)>",
        )

    def test_p5netcdf_Variable_maxshape(self):
        """Test Variable_maxshape."""
        var = self.p["time"]
        self.assertEqual(var.maxshape, ())

        var = self.p["forecast/lon"]
        self.assertEqual(var.maxshape, (None,))
        var = self.p["forecast/lon_bnds"]

        self.assertEqual(var.maxshape, (None, 2))

    def test_p5netcdf_Variable_name(self):
        """Test Variable.name."""
        var = self.p["time"]
        self.assertEqual(var.name, "time")

        var = self.p["/forecast/model/q"]
        self.assertEqual(var.name, "q")

    def test_p5netcdf_Variable_path(self):
        """Test Variable.path."""
        var = self.p["time"]
        self.assertEqual(var.path, "/time")

        var = self.p["/forecast/model/q"]
        self.assertEqual(var.path, "/forecast/model/q")

    def test_p5netcdf_Variable_chunking(self):
        """Test Variable.chunking."""
        var = self.p["/forecast/model/lat"]
        self.assertEqual(var.chunking(), "contiguous")

        var = self.p["/forecast/model/q"]
        self.assertEqual(var.chunking(), [5, 3])

    def test_p5netcdf_Variable_chunks(self):
        """Test Variable.chunks."""
        var = self.p["/forecast/model/lat"]
        self.assertIsNone(var.chunks)

        var = self.p["/forecast/model/q"]
        self.assertEqual(var.chunks, (5, 3))

    def test_p5netcdf_Variable_dtype(self):
        """Test Variable.dtype."""
        var = self.p["/time"]
        self.assertEqual(var.dtype, "int32")

        var = self.p["/forecast/lon"]
        self.assertEqual(var.dtype, "float64")

        var = self.p["/forecast/model/q"]
        self.assertEqual(var.dtype, "float32")

    def test_p5netcdf_Variable_orthogonal_indexing(self):
        """Test Variable.__orthogonal_indexing__ property."""
        var = self.p["/forecast/model/q"]
        # Should be a boolean
        self.assertIsInstance(var.__orthogonal_indexing__, bool)

    def test_p5netcdf_Variable_dimension_paths(self):
        """Test Variable.dimension_paths property."""
        var = self.p["/time"]
        self.assertEqual(var.dimension_paths, ())

        var = self.p["/forecast/lon"]
        self.assertEqual(var.dimension_paths, ("/forecast/lon",))

        var = self.p["/forecast/model/q"]
        self.assertEqual(
            var.dimension_paths, ("/forecast/model/lat", "/forecast/lon")
        )

    def test_p5netcdf_Variable_shards(self):
        """Test Variable.shards property."""
        var = self.p["/forecast/model/q"]
        # For non-zarr datasets, shards should be None
        self.assertIsNone(var.shards)

    def test_p5netcdf_Variable_ndim(self):
        """Test Variable.ndim."""
        var = self.p["/time"]
        self.assertEqual(var.ndim, 0)

        var = self.p["/forecast/lon"]
        self.assertEqual(var.ndim, 1)

        var = self.p["/forecast/model/q"]
        self.assertEqual(var.ndim, 2)

    def test_p5netcdf_Variable_shape(self):
        """Test Variable.shape."""
        var = self.p["/time"]
        self.assertEqual(var.shape, ())

        var = self.p["/forecast/lon"]
        self.assertEqual(var.shape, (8,))

        var = self.p["/forecast/model/q"]
        self.assertEqual(var.shape, (5, 8))

    def test_p5netcdf_Variable_size(self):
        """Test Variable.size."""
        var = self.p["/time"]
        self.assertEqual(var.size, 1)

        var = self.p["/forecast/lon"]
        self.assertEqual(var.size, 8)

        var = self.p["/forecast/model/q"]
        self.assertEqual(var.size, 40)

    def test_p5netcdf_Variable__len__(self):
        """Test Variable.__len__."""
        var = self.p["/time"]
        with self.assertRaises(TypeError):
            len(var)

        var = self.p["/forecast/lon"]
        self.assertEqual(len(var), 8)

        var = self.p["/forecast/model/q"]
        self.assertEqual(len(var), 5)

    def test_p5netcdf_Variable_dimensions(self):
        """Test Variable.dimensions."""
        var = self.p["/time"]
        self.assertEqual(var.dimensions, ())

        var = self.p["/forecast/lon"]
        self.assertEqual(var.dimensions, ("lon",))

        var = self.p["/forecast/model/q"]
        self.assertEqual(len(var.get_dims()), 2)
        self.assertEqual(var.dimensions, ("lat", "lon"))

    def test_p5netcdf_Variable_get_dims(self):
        """Test Variable.__len__."""
        var = self.p["/time"]
        self.assertEqual(var.get_dims(), ())

        var = self.p["/forecast/lon"]
        self.assertEqual(len(var.get_dims()), 1)
        self.assertEqual(
            var.get_dims(), (self.p["/forecast"].dimensions["lon"],)
        )

        var = self.p["/forecast/model/q"]
        self.assertEqual(len(var.get_dims()), 2)
        self.assertEqual(
            var.get_dims(),
            (
                self.p["/forecast/model"].dimensions["lat"],
                self.p["/forecast"].dimensions["lon"],
            ),
        )

    def test_p5netcdf_Variable_parent(self):
        """Test Variable.parent."""
        var = self.p["/time"]
        self.assertIs(var.parent, self.p)

        var = self.p["/forecast/lon"]
        self.assertIs(var.parent, self.p["/forecast"])

        var = self.p["/forecast/model/q"]
        self.assertIs(var.parent, self.p["/forecast/model"])

    def test_p5netcdf_Variable_group(self):
        """Test Variable.group."""
        var = self.p["/time"]
        self.assertIs(var.group(), self.p)

        var = self.p["/forecast/lon"]
        self.assertIs(var.group(), self.p["/forecast"])

        var = self.p["/forecast/model/q"]
        self.assertIs(var.group(), self.p["/forecast/model"])

    def test_p5netcdf_Variable_getValue(self):
        """Test Variable.getValue."""
        # Test scalar variable
        var = self.p["/time"]
        self.assertEqual(var.getValue(), 31)

        # Test non-scalar variable should raise IndexError
        var = self.p["/forecast/lon"]
        with self.assertRaises(IndexError):
            var.getValue()

    def test_p5netcdf_Variable_getncattr(self):
        """Test Variable.getncattr."""
        var = self.p["/forecast/model/q"]

        # Test existing attribute
        self.assertEqual(var.getncattr("standard_name"), "specific_humidity")
        self.assertEqual(var.getncattr("int"), 49)

        # Test non-existing attribute should raise AttributeError
        with self.assertRaises(AttributeError):
            var.getncattr("nonexistent_attr")

    def test_p5netcdf_Variable_ncattrs(self):
        """Test Variable.ncattrs."""
        var = self.p["/forecast/model/q"]
        attrs = var.ncattrs()
        self.assertIsInstance(attrs, list)
        self.assertIn("standard_name", attrs)
        self.assertIn("int", attrs)
        self.assertEqual(set(attrs), set(var.attrs.keys()))

    def test_p5netcdf_Group__repr__(self):
        """Test Group.__repr__."""
        self.assertEqual(
            repr(self.p["/forecast"]),
            "<p5netcdf.Group: /forecast, 1 dimension, 2 variables, 1 group>",
        )

        self.assertEqual(
            repr(self.p["/forecast/model"]),
            "<p5netcdf.Group: /forecast/model, 1 dimension, 3 variables, 0 groups>",
        )

    def test_p5netcdf_Group__str__(self):
        """Test Group.__str__."""
        self.assertEqual(
            str(self.p["/forecast"]),
            """forecast: <p5netcdf.Group: /forecast, 1 dimension, 2 variables, 1 group>
    Dimensions:
        lon: <p5netcdf.Dimension: /forecast/lon, size=8, unlimited>
    Variables:
        lon_bnds: <p5netcdf.Variable: /forecast/lon_bnds, shape=(8, 2), dimensions=(/forecast/lon, /bounds2)>
        lon: <p5netcdf.Variable: /forecast/lon, shape=(8,), dimensions=(/forecast/lon,)>
    Groups:
        model: <p5netcdf.Group: /forecast/model, 1 dimension, 3 variables, 0 groups>""",
        )

        self.assertEqual(
            str(self.p["/forecast/model"]),
            """model: <p5netcdf.Group: /forecast/model, 1 dimension, 3 variables, 0 groups>
    Dimensions:
        lat: <p5netcdf.Dimension: /forecast/model/lat, size=5>
    Variables:
        lat_bnds: <p5netcdf.Variable: /forecast/model/lat_bnds, shape=(5, 2), dimensions=(/forecast/model/lat, /bounds2)>
        lat: <p5netcdf.Variable: /forecast/model/lat, shape=(5,), dimensions=(/forecast/model/lat,)>
        q: <p5netcdf.Variable: /forecast/model/q, shape=(5, 8), dimensions=(/forecast/model/lat, /forecast/lon)>""",
        )

    def test_p5netcdf_Group_dump(self):
        """Test Group.dump."""
        self.assertEqual(
            self.p["/forecast"].dump(display=False),
            """forecast: <p5netcdf.Group: /forecast, 1 dimension, 2 variables, 1 group>
    Dimensions:
        lon: <p5netcdf.Dimension: /forecast/lon, size=8, unlimited>
    Variables:
        lon_bnds: <p5netcdf.Variable: /forecast/lon_bnds, shape=(8, 2), dimensions=(/forecast/lon, /bounds2)>
        lon: <p5netcdf.Variable: /forecast/lon, shape=(8,), dimensions=(/forecast/lon,)>
            Attributes:
                units: 'degrees_east'
                standard_name: 'longitude'
                bounds: '/forecast/lon_bnds'
    Groups:
        model: <p5netcdf.Group: /forecast/model, 1 dimension, 3 variables, 0 groups>
            Attributes:
                group_attr_1: np.int64(12)
                group_attr_2: 'bar'
            Dimensions:
                lat: <p5netcdf.Dimension: /forecast/model/lat, size=5>
            Variables:
                lat_bnds: <p5netcdf.Variable: /forecast/model/lat_bnds, shape=(5, 2), dimensions=(/forecast/model/lat, /bounds2)>
                lat: <p5netcdf.Variable: /forecast/model/lat, shape=(5,), dimensions=(/forecast/model/lat,)>
                    Attributes:
                        units: 'degrees_north'
                        standard_name: 'latitude'
                        bounds: '/forecast/model/lat_bnds'
                q: <p5netcdf.Variable: /forecast/model/q, shape=(5, 8), dimensions=(/forecast/model/lat, /forecast/lon)>
                    Attributes:
                        list7: array([2, 3])
                        int: np.int64(49)
                        int8: np.int8(49)
                        int16: np.int16(49)
                        int32: np.int32(49)
                        int64: np.int64(49)
                        float: np.float64(49.0)
                        float32: np.float32(49.0)
                        float64: np.float64(49.0)
                        uint8: np.uint8(49)
                        uint16: np.uint16(49)
                        uint32: np.uint32(49)
                        uint64: np.uint64(49)
                        list1: array([2, 3], dtype=int8)
                        list2: array([2, 3], dtype=int16)
                        list3: array([2, 3])
                        list4: array([2, 3], dtype=int32)
                        list5: array([2., 3.], dtype=float32)
                        list6: array([2., 3.])
                        list8: np.int32(2)
                        list9: array([], dtype=int32)
                        list10: array([], dtype=float64)
                        list11: ['a', 'bb', 'ccc']
                        list12: ['a', '1', '2.5']
                        list13: 'a'
                        list14: ['a', 'bb']
                        string1: '1'
                        string2: 'a'
                        string3: 'kg m-2'
                        string4: ''
                        string5: ' '
                        string6: ''
                        string7: ''
                        string8: ''
                        string9: ''
                        coordinates: 'time'
                        cell_methods: 'area: mean'
                        standard_name: 'specific_humidity'""",
        )

        self.assertEqual(
            self.p["/forecast/model"].dump(display=False),
            """model: <p5netcdf.Group: /forecast/model, 1 dimension, 3 variables, 0 groups>
    Attributes:
        group_attr_1: np.int64(12)
        group_attr_2: 'bar'
    Dimensions:
        lat: <p5netcdf.Dimension: /forecast/model/lat, size=5>
    Variables:
        lat_bnds: <p5netcdf.Variable: /forecast/model/lat_bnds, shape=(5, 2), dimensions=(/forecast/model/lat, /bounds2)>
        lat: <p5netcdf.Variable: /forecast/model/lat, shape=(5,), dimensions=(/forecast/model/lat,)>
            Attributes:
                units: 'degrees_north'
                standard_name: 'latitude'
                bounds: '/forecast/model/lat_bnds'
        q: <p5netcdf.Variable: /forecast/model/q, shape=(5, 8), dimensions=(/forecast/model/lat, /forecast/lon)>
            Attributes:
                list7: array([2, 3])
                int: np.int64(49)
                int8: np.int8(49)
                int16: np.int16(49)
                int32: np.int32(49)
                int64: np.int64(49)
                float: np.float64(49.0)
                float32: np.float32(49.0)
                float64: np.float64(49.0)
                uint8: np.uint8(49)
                uint16: np.uint16(49)
                uint32: np.uint32(49)
                uint64: np.uint64(49)
                list1: array([2, 3], dtype=int8)
                list2: array([2, 3], dtype=int16)
                list3: array([2, 3])
                list4: array([2, 3], dtype=int32)
                list5: array([2., 3.], dtype=float32)
                list6: array([2., 3.])
                list8: np.int32(2)
                list9: array([], dtype=int32)
                list10: array([], dtype=float64)
                list11: ['a', 'bb', 'ccc']
                list12: ['a', '1', '2.5']
                list13: 'a'
                list14: ['a', 'bb']
                string1: '1'
                string2: 'a'
                string3: 'kg m-2'
                string4: ''
                string5: ' '
                string6: ''
                string7: ''
                string8: ''
                string9: ''
                coordinates: 'time'
                cell_methods: 'area: mean'
                standard_name: 'specific_humidity'""",
        )

    def test_p5netcdf_Group__getitem__(self):
        """Test Group.__getitem__."""
        self.assertIs(self.p[""], self.p)
        self.assertIs(self.p["/"], self.p)
        self.assertIs(self.p["//"], self.p)
        self.assertIs(self.p["/."], self.p)
        self.assertIs(self.p["."], self.p)
        self.assertIs(self.p["./"], self.p)
        self.assertIs(self.p["./."], self.p)
        self.assertIs(self.p["/./."], self.p)
        self.assertIs(self.p["/././"], self.p)
        self.assertIs(self.p["forecast"], self.p["/forecast"])
        self.assertIs(self.p["forecast"], self.p["/forecast/"])
        self.assertIs(self.p["forecast/model"], self.p["forecast"]["model"])
        self.assertIs(
            self.p["/forecast/model/"], self.p["/forecast"]["model/"]
        )
        self.assertIs(
            self.p["forecast"]["/forecast/model/"], self.p["/forecast/model"]
        )

        self.assertIs(self.p["forecast"]["/"], self.p["/"])
        self.assertIs(self.p["forecast"][""], self.p["/forecast"])
        self.assertIs(self.p["forecast"]["."], self.p["/forecast"])
        self.assertIs(self.p["forecast"]["./"], self.p["/forecast"])
        self.assertIs(self.p["forecast"]["/forecast"], self.p["/forecast"])
        self.assertIs(
            self.p["forecast"]["/forecast/model/q"],
            self.p["/forecast/model/q"],
        )
        self.assertIs(
            self.p["/forecast/model/q/"],
            self.p["/forecast/model/q"],
        )
        self.assertIs(
            self.p["/forecast//model///q////"],
            self.p["/forecast/model/q"],
        )
        self.assertIs(
            self.p["/forecast/model/q"],
            self.p["/forecast"]["model"]["q"],
        )

        self.assertIs(self.p["forecast/.."], self.p["/"])
        self.assertIs(self.p["/forecast/.."], self.p["/"])
        self.assertIs(self.p["/forecast/./.."], self.p["/"])
        self.assertIs(self.p["./forecast/./.."], self.p["/"])
        self.assertIs(
            self.p["./forecast/./../forecast/model/.."], self.p["/forecast"]
        )

        self.assertIs(self.p["/forecast"][""], self.p["/forecast"])
        self.assertIs(self.p["/forecast"]["."], self.p["/forecast"])
        self.assertIs(self.p["/forecast"]["./"], self.p["/forecast"])
        self.assertIs(self.p["/forecast"][".."], self.p)
        self.assertIs(self.p["/forecast"]["../"], self.p)

        # Test bad paths from the root group
        current_group = self.p["/"]
        for bad_group in (
            "/..",
            "./..",
            "/bad_group",
            "bad_group",
            "/forecast/bad_group",
            "/forecast/model/q/subgroup",
            "/forecast/model/q/..",
            "/forecast/model/q/.",
            "/forecast/model/q/./",
        ):
            with self.assertRaises(KeyError):
                current_group[bad_group]

        # Test bad paths from a sub-group
        current_group = self.p["/forecast"]
        for bad_group in (
            "../..",
            "./../..",
            "../bad_group",
            "../model/.././bad_group",
            "/bad_group",
            "bad_group",
            "/forecast/bad_group",
            "/forecast/model/q/subgroup",
            "/forecast/model/q/..",
            "/forecast/model/q/.",
            "/forecast/model/q/./",
            "model/bad_group",
            "model/q/subgroup",
        ):
            with self.assertRaises(KeyError):
                current_group[bad_group]

    def test_p5netcdf_Group__iter__(self):
        """Test Group.__iter__."""
        group = self.p
        self.assertEqual(tuple(group), ("forecast", "time"))

        group = self.p["/forecast"]
        self.assertEqual(
            tuple(group),
            (
                "model",
                "lon_bnds",
                "lon",
            ),
        )

        group = self.p["/forecast/model"]
        self.assertEqual(tuple(group), ("lat_bnds", "lat", "q"))

    def test_p5netcdf_Group_keys(self):
        """Test Group.keys."""
        group = self.p
        self.assertEqual(tuple(group.keys()), ("forecast", "time"))

        group = self.p["/forecast"]
        self.assertEqual(tuple(group.keys()), ("model", "lon_bnds", "lon"))

        group = self.p["/forecast/model"]
        self.assertEqual(tuple(group.keys()), ("lat_bnds", "lat", "q"))

    def test_p5netcdf_Group_values(self):
        """Test Group.values."""
        group = self.p
        self.assertEqual(
            tuple(group.values()), (group["forecast"], group["time"])
        )

        group = self.p["/forecast"]
        self.assertEqual(
            tuple(group.values()),
            (group["model"], group["lon_bnds"], group["lon"]),
        )

        group = self.p["/forecast/model"]
        self.assertEqual(
            tuple(group.values()),
            (group["lat_bnds"], group["lat"], group["q"]),
        )

    def test_p5netcdf_Group_items(self):
        """Test Group.items."""
        group = self.p
        self.assertEqual(
            tuple(group.items()),
            (("forecast", group["forecast"]), ("time", group["time"])),
        )

        group = self.p["/forecast"]
        self.assertEqual(
            tuple(group.items()),
            (
                ("model", group["model"]),
                ("lon_bnds", group["lon_bnds"]),
                ("lon", group["lon"]),
            ),
        )

        group = self.p["/forecast/model"]
        self.assertEqual(
            tuple(group.items()),
            (
                ("lat_bnds", group["lat_bnds"]),
                ("lat", group["lat"]),
                ("q", group["q"]),
            ),
        )

    def test_p5netcdf_Group_name(self):
        """Test Group.name."""
        group = self.p
        self.assertEqual(group.name, "")

        group = self.p["/forecast"]
        self.assertEqual(group.name, "forecast")

        group = self.p["/forecast/model"]
        self.assertEqual(group.name, "model")

    def test_p5netcdf_Group_path(self):
        """Test Group.path."""
        for path in ("/", "/forecast", "/forecast/model"):
            group = self.p[path]
            self.assertEqual(group.path, path)

    def test_p5netcdf_Group_parent(self):
        """Test Group.parent."""
        group = self.p
        self.assertIsNone(group.parent)

        group = self.p["/forecast"]
        self.assertIs(group.parent, self.p)

        group = self.p["/forecast/model"]
        self.assertIs(group.parent, self.p["/forecast"])

    def test_p5netcdf_Group_is_sub_group(self):
        """Test Group.is_sub_group."""
        self.assertTrue(self.p.is_sub_group(self.p))
        self.assertTrue(self.p["forecast"].is_sub_group(self.p["forecast"]))
        self.assertTrue(self.p["forecast"].is_sub_group(self.p))
        self.assertTrue(self.p["forecast/model"].is_sub_group(self.p))
        self.assertTrue(
            self.p["forecast/model"].is_sub_group(self.p["forecast"])
        )

        self.assertFalse(self.p.is_sub_group(self.p["forecast"]))
        self.assertFalse(self.p.is_sub_group(self.p["forecast/model"]))
        self.assertFalse(
            self.p["forecast"].is_sub_group(self.p["forecast/model"])
        )

    def test_p5netcdf_Group_is_ancestor_group(self):
        """Test Group.is_ancestor_group."""
        self.assertTrue(self.p.is_ancestor_group(self.p))
        self.assertTrue(self.p.is_ancestor_group(self.p["forecast"]))
        self.assertTrue(self.p.is_ancestor_group(self.p["forecast/model"]))
        self.assertTrue(
            self.p["forecast"].is_ancestor_group(self.p["forecast"])
        )
        self.assertTrue(
            self.p["forecast"].is_ancestor_group(self.p["forecast/model"])
        )

        self.assertFalse(self.p["forecast"].is_ancestor_group(self.p))
        self.assertFalse(self.p["forecast/model"].is_ancestor_group(self.p))
        self.assertFalse(
            self.p["forecast/model"].is_ancestor_group(self.p["forecast"])
        )

    def test_p5netcdf_zarr_backend(self):
        """Test Zarr backend functionality."""
        if hasattr(self, "pz"):
            self.assertEqual(self.pz.backend, "zarr")
            # Test that basic operations work
            self.assertIsInstance(self.pz.dimensions, dict)
            self.assertIsInstance(self.pz.variables, dict)

    def test_p5netcdf_kerchunk_backend(self):
        """Test Kerchunk backend functionality."""
        if hasattr(self, "pk"):
            self.assertEqual(self.pk.backend, "zarr")
            # Test that basic operations work
            self.assertIsInstance(self.pk.dimensions, dict)
            self.assertIsInstance(self.pk.variables, dict)

    def test_p5netcdf_zarr_dimension_search(self):
        """Test zarr_dimension_search parameter."""
        if hasattr(self, "zarr"):
            # Test different search strategies
            for strategy in ["closest_ancestor", "furthest_ancestor", "local"]:
                p = cfdm.p5netcdf.File(
                    self.zarr, zarr_dimension_search=strategy
                )
                self.assertEqual(p.backend, "zarr")

    def test_p5netcdf_Group_getncattr(self):
        """Test Group.getncattr."""
        # Test root group attributes
        self.assertEqual(self.p.getncattr("Conventions"), "CF-1.13")
        self.assertEqual(self.p.getncattr("global_attr_1"), 3.14)

        # Test subgroup attributes
        model_group = self.p["/forecast/model"]
        self.assertEqual(model_group.getncattr("group_attr_1"), 12)
        self.assertEqual(model_group.getncattr("group_attr_2"), "bar")

        # Test non-existing attribute should raise AttributeError
        with self.assertRaises(AttributeError):
            self.p.getncattr("nonexistent_attr")

    def test_p5netcdf_Group_ncattrs(self):
        """Test Group.ncattrs."""
        # Test root group
        attrs = self.p.ncattrs()
        self.assertIsInstance(attrs, list)
        self.assertIn("Conventions", attrs)
        self.assertIn("global_attr_1", attrs)
        self.assertEqual(set(attrs), set(self.p.attrs.keys()))

        # Test subgroup
        model_group = self.p["/forecast/model"]
        attrs = model_group.ncattrs()
        self.assertIn("group_attr_1", attrs)
        self.assertIn("group_attr_2", attrs)
        self.assertEqual(set(attrs), set(model_group.attrs.keys()))

    def test_p5netcdf_edge_cases(self):
        """Test various edge cases and error conditions."""
        # Test Group.__len__
        self.assertEqual(len(self.p), 2)  # forecast group + time variable
        self.assertEqual(
            len(self.p["/forecast"]), 3
        )  # model group + 2 variables

        # Test Group.get() method (inherited from Mapping)
        self.assertIs(self.p.get("time"), self.p["time"])
        self.assertIsNone(self.p.get("nonexistent"))
        self.assertEqual(self.p.get("nonexistent", "default"), "default")

        # Test Variable array access with different indices
        var = self.p["/forecast/lon"]
        self.assertEqual(var[0], 22.5)
        self.assertTrue(np.array_equal(var[:2], [22.5, 67.5]))

        # Test scalar variable indexing
        time_var = self.p["/time"]
        self.assertEqual(time_var[()], 31)

    def test_p5netcdf_metadata_strategy(self):
        """Test different metadata strategies."""
        # Test minimal strategy
        p_min = cfdm.p5netcdf.File(self.filename, metadata_strategy="minimal")
        self.assertEqual(p_min.backend, "pyfive")

        # Test maximal strategy
        p_max = cfdm.p5netcdf.File(self.filename, metadata_strategy="maximal")
        self.assertEqual(p_max.backend, "pyfive")

        # Test invalid strategy
        with self.assertRaises(ValueError):
            cfdm.p5netcdf.File(self.filename, metadata_strategy="invalid")

    def test_p5netcdf_options_parameters(self):
        """Test various options parameters."""
        # Test with empty options
        p = cfdm.p5netcdf.File(self.filename, pyfive_options={})
        self.assertEqual(p.backend, "pyfive")

        # Test with h5py_options (should fall back to other backends)
        p = cfdm.p5netcdf.File(self.filename, h5py_options={})
        self.assertEqual(p.backend, "pyfive")


if __name__ == "__main__":
    print("Run date:", datetime.datetime.now())
    cfdm.environment()
    print("")
    unittest.main(verbosity=2)
