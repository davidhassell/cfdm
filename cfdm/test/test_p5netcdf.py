import datetime
import os
import pathlib
import tempfile
import unittest

import fsspec
import h5py
import netCDF4
import numpy as np
import pyfive

import cfdm


class Testp5netcdf(unittest.TestCase):
    """Test suite for the p5netcdf read-only netCDF implementation."""

    @classmethod
    def setUpClass(cls):
        """Create the test file."""
        # tmpfile = tempfile.mkstemp("_test_p5netcdf.nc", dir=os.getcwd())[1]
        tmpfile = "test_p5_example.nc"

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
        cls.p5 = cfdm.p5netcdf.File(tmpfile)

    # @classmethod
    # def tearDownClass(cls):
    #    """Clean up the generated test file."""
    #    if os.path.exists(cls.filename):
    #        os.remove(cls.filename)

    def test_p5netcdf_attributes(self):
        """Check that attributes are parsed correctly."""
        n = netCDF4.Dataset(self.filename, "r")
        p = self.p5

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

        n.close()

    def test_p5netcdf_dimensions(self):
        """Check that dimensions are parsed correctly."""
        n = netCDF4.Dataset(self.filename, "r")
        p = self.p5

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

        n.close()

    def test_p5netcdf_variables(self):
        """Check that variables are parsed correctly."""
        n = netCDF4.Dataset(self.filename, "r")
        p = self.p5

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

        n.close()

    def test_p5netcdf_groups(self):
        """Check that groups are parsed correctly."""
        n = netCDF4.Dataset(self.filename, "r")
        p = self.p5

        for group in ("/", "/forecast", "/forecast/model"):
            pg = p[group]
            ng = n if group == "/" else n[group]

            pattrs = pg.attrs
            nattrs = {k: ng.getncattr(k) for k in ng.ncattrs()}
            self.assertEqual(pattrs, nattrs)
            self.assertEqual(pg.path, ng.path)

        n.close()

    def test_p5netcdf_File_Path(self):
        """Check File with a file-like input."""
        p5p = cfdm.p5netcdf.File(pathlib.Path(self.filename))
        self.assertEqual(
            p5p["forecast/model/q"].dimensions,
            self.p5["forecast/model/q"].dimensions,
        )

    def test_p5netcdf_File_file_like(self):
        """Check File with a file-like input."""
        local_fs = fsspec.filesystem("local")
        fh = local_fs.open(self.filename, "rb")
        p5fh = cfdm.p5netcdf.File(fh)
        self.assertEqual(
            p5fh["forecast/model/q"].dimensions,
            self.p5["forecast/model/q"].dimensions,
        )

    def test_p5netcdf_File_pyfive_like(self):
        """Check File with a pyfive.File input."""
        py5 = pyfive.File(self.filename)
        p5py5 = cfdm.p5netcdf.File(py5)
        self.assertEqual(
            p5py5["forecast/model/q"].dimensions,
            self.p5["forecast/model/q"].dimensions,
        )

    def test_p5netcdf_File__repr__(self):
        """Test File.__repr__."""
        self.assertEqual(
            repr(self.p5), "<p5netcdf.File: 1 variable, 1 sub-group>"
        )

    def test_p5netcdf_File__str__(self):
        """Test File.__str__."""
        self.assertEqual(
            str(self.p5),
            """<p5netcdf.File: 1 variable, 1 sub-group>
Attributes:
    Conventions: 'CF-1.13'
    global_attr_1: np.float64(3.14)
    global_attr_2: 'foo'
Groups:
    forecast
Dimensions:
    bounds2: <p5netcdf.Dimension: /bounds2, size=2>
Variables:
    time: <p5netcdf.Variable: /time, shape=(), dimensions=()>""",
        )

    def test_p5netcdf_File_close(self):
        """Test File.close."""
        p = cfdm.p5netcdf.File(self.p5.filename)
        self.assertFalse(p._h5._fh.closed)
        p.close()
        self.assertTrue(p._h5._fh.closed)

        py5 = pyfive.File(self.p5.filename)
        p = cfdm.p5netcdf.File(py5)
        self.assertFalse(p._h5._fh.closed)
        p.close()
        self.assertFalse(p._h5._fh.closed)

    def test_p5netcdf_File_filename(self):
        """Test File.filename."""
        self.assertEqual(self.p5.filename, self.filename)

    def test_p5netcdf_File_enter_exit(self):
        """Test File in context manager."""
        with cfdm.p5netcdf.File(self.p5.filename) as p:
            self.assertEqual(p.attrs["global_attr_2"], "foo")
            self.assertFalse(p._h5._fh.closed)

        self.assertTrue(p._h5._fh.closed)

        py5 = pyfive.File(self.p5.filename)
        self.assertFalse(py5._fh.closed)
        with cfdm.p5netcdf.File(py5) as p:
            self.assertEqual(p.attrs["global_attr_2"], "foo")

        self.assertFalse(py5._fh.closed)

    def test_p5netcdf_Dimension__repr__(self):
        """Test Dimension.__repr__."""
        dim = self.p5.dimensions["bounds2"]
        self.assertEqual(repr(dim), "<p5netcdf.Dimension: /bounds2, size=2>")

        dim = self.p5["forecast"].dimensions["lon"]
        self.assertEqual(
            repr(dim),
            "<p5netcdf.Dimension: /forecast/lon, size=8, unlimited>",
        )

    def test_p5netcdf_Dimension_size(self):
        """Test Dimension.size."""
        dim = self.p5.dimensions["bounds2"]
        self.assertFalse(dim.isunlimited())
        self.assertEqual(dim.size, 2)

        dim = self.p5["forecast"].dimensions["lon"]
        self.assertTrue(dim.isunlimited())
        self.assertEqual(len(dim), 8)

    def test_p5netcdf_Dimension__len__(self):
        """Test Dimension.__len__."""
        dim = self.p5.dimensions["bounds2"]
        self.assertFalse(dim.isunlimited())
        self.assertEqual(len(dim), 2)

        dim = self.p5["forecast"].dimensions["lon"]
        self.assertTrue(dim.isunlimited())
        self.assertEqual(len(dim), 8)

    def test_p5netcdf_Dimension_isunlimited(self):
        """Test Dimension.isunlimited."""
        dim = self.p5.dimensions["bounds2"]
        self.assertIsInstance(dim.isunlimited(), bool)
        self.assertFalse(dim.isunlimited())

        dim = self.p5["forecast"].dimensions["lon"]
        self.assertIsInstance(dim.isunlimited(), bool)
        self.assertTrue(dim.isunlimited())

    def test_p5netcdf_Dimension_name(self):
        """Test Dimension.name."""
        dim = self.p5.dimensions["bounds2"]
        self.assertEqual(dim.name, "bounds2")

        dim = self.p5["forecast"].dimensions["lon"]
        self.assertEqual(dim.name, "lon")

    def test_p5netcdf_Dimension_path(self):
        """Test Dimension.path."""
        dim = self.p5.dimensions["bounds2"]
        self.assertEqual(dim.path, "/bounds2")

        dim = self.p5["forecast"].dimensions["lon"]
        self.assertEqual(dim.path, "/forecast/lon")

    def test_p5netcdf_Dimension_group(self):
        """Test Dimension.group."""
        group = self.p5
        dim = group.dimensions["bounds2"]
        self.assertIs(dim.group(), group)

        group = self.p5["/forecast"]
        dim = group.dimensions["lon"]
        self.assertIs(dim.group(), group)

        group = self.p5["/forecast/model"]
        dim = group.dimensions["lat"]
        self.assertIs(dim.group(), group)

    def test_p5netcdf_Dimension_parent(self):
        """Test Dimension.group."""
        group = self.p5
        dim = group.dimensions["bounds2"]
        self.assertIs(dim.parent, group)

        group = self.p5["/forecast"]
        dim = group.dimensions["lon"]
        self.assertIs(dim.parent, group)

        group = self.p5["/forecast/model"]
        dim = group.dimensions["lat"]
        self.assertIs(dim.parent, group)

    def test_p5netcdf_Variable__repr__(self):
        """Test Variable.__repr__."""
        var = self.p5["time"]
        self.assertEqual(
            repr(var), "<p5netcdf.Variable: /time, shape=(), dimensions=()>"
        )

        var = self.p5["forecast/lon"]
        self.assertEqual(
            repr(var),
            "<p5netcdf.Variable: /forecast/lon, shape=(8,), dimensions=(/forecast/lon,)>",
        )

        var = self.p5["/forecast/model/q"]
        self.assertEqual(
            repr(var),
            "<p5netcdf.Variable: /forecast/model/q, shape=(5, 8), dimensions=(/forecast/model/lat, /forecast/lon)>",
        )

    def test_p5netcdf_Variable_maxshape(self):
        """Test Variable_maxshape."""
        var = self.p5["time"]
        self.assertEqual(var.maxshape, ())

        var = self.p5["forecast/lon"]
        self.assertEqual(var.maxshape, (None,))
        var = self.p5["forecast/lon_bnds"]

        self.assertEqual(var.maxshape, (None, 2))

    def test_p5netcdf_Variable_name(self):
        """Test Variable.name."""
        var = self.p5["time"]
        self.assertEqual(var.name, "time")

        var = self.p5["/forecast/model/q"]
        self.assertEqual(var.name, "q")

    def test_p5netcdf_Variable_path(self):
        """Test Variable.path."""
        var = self.p5["time"]
        self.assertEqual(var.path, "/time")

        var = self.p5["/forecast/model/q"]
        self.assertEqual(var.path, "/forecast/model/q")

    def test_p5netcdf_Variable_chunking(self):
        """Test Variable.chunking."""
        var = self.p5["/forecast/model/lat"]
        self.assertEqual(var.chunking(), "contiguous")

        var = self.p5["/forecast/model/q"]
        self.assertEqual(var.chunking(), [5, 3])

    def test_p5netcdf_Variable_chunks(self):
        """Test Variable.chunks."""
        var = self.p5["/forecast/model/lat"]
        self.assertIsNone(var.chunks)

        var = self.p5["/forecast/model/q"]
        self.assertEqual(var.chunks, (5, 3))

    def test_p5netcdf_Variable_dtype(self):
        """Test Variable.dtype."""
        var = self.p5["/time"]
        self.assertEqual(var.dtype, "int32")

        var = self.p5["/forecast/lon"]
        self.assertEqual(var.dtype, "float64")

        var = self.p5["/forecast/model/q"]
        self.assertEqual(var.dtype, "float32")

    def test_p5netcdf_Variable_ndim(self):
        """Test Variable.ndim."""
        var = self.p5["/time"]
        self.assertEqual(var.ndim, 0)

        var = self.p5["/forecast/lon"]
        self.assertEqual(var.ndim, 1)

        var = self.p5["/forecast/model/q"]
        self.assertEqual(var.ndim, 2)

    def test_p5netcdf_Variable_shape(self):
        """Test Variable.shape."""
        var = self.p5["/time"]
        self.assertEqual(var.shape, ())

        var = self.p5["/forecast/lon"]
        self.assertEqual(var.shape, (8,))

        var = self.p5["/forecast/model/q"]
        self.assertEqual(var.shape, (5, 8))

    def test_p5netcdf_Variable_size(self):
        """Test Variable.size."""
        var = self.p5["/time"]
        self.assertEqual(var.size, 1)

        var = self.p5["/forecast/lon"]
        self.assertEqual(var.size, 8)

        var = self.p5["/forecast/model/q"]
        self.assertEqual(var.size, 40)

    def test_p5netcdf_Variable__len__(self):
        """Test Variable.__len__."""
        var = self.p5["/time"]
        with self.assertRaises(TypeError):
            len(var)

        var = self.p5["/forecast/lon"]
        self.assertEqual(len(var), 8)

        var = self.p5["/forecast/model/q"]
        self.assertEqual(len(var), 5)

    def test_p5netcdf_Variable_dimensions(self):
        """Test Variable.dimensions."""
        var = self.p5["/time"]
        self.assertEqual(var.dimensions, ())

        var = self.p5["/forecast/lon"]
        self.assertEqual(var.dimensions, ("lon",))

        var = self.p5["/forecast/model/q"]
        self.assertEqual(len(var.get_dims()), 2)
        self.assertEqual(var.dimensions, ("lat", "lon"))

    def test_p5netcdf_Variable_get_dims(self):
        """Test Variable.__len__."""
        var = self.p5["/time"]
        self.assertEqual(var.get_dims(), ())

        var = self.p5["/forecast/lon"]
        self.assertEqual(len(var.get_dims()), 1)
        self.assertEqual(
            var.get_dims(), (self.p5["/forecast"].dimensions["lon"],)
        )

        var = self.p5["/forecast/model/q"]
        self.assertEqual(len(var.get_dims()), 2)
        self.assertEqual(
            var.get_dims(),
            (
                self.p5["/forecast/model"].dimensions["lat"],
                self.p5["/forecast"].dimensions["lon"],
            ),
        )

    def test_p5netcdf_Variable_parent(self):
        """Test Variable.parent."""
        var = self.p5["/time"]
        self.assertIs(var.parent, self.p5)

        var = self.p5["/forecast/lon"]
        self.assertIs(var.parent, self.p5["/forecast"])

        var = self.p5["/forecast/model/q"]
        self.assertIs(var.parent, self.p5["/forecast/model"])

    def test_p5netcdf_Variable_group(self):
        """Test Variable.group."""
        var = self.p5["/time"]
        self.assertIs(var.group(), self.p5)

        var = self.p5["/forecast/lon"]
        self.assertIs(var.group(), self.p5["/forecast"])

        var = self.p5["/forecast/model/q"]
        self.assertIs(var.group(), self.p5["/forecast/model"])

    def test_p5netcdf_Group__repr__(self):
        """Test Group.__repr__."""
        group = self.p5["/forecast"]
        self.assertEqual(
            repr(group),
            "<p5netcdf.Group: /forecast, 2 variables, 1 sub-group>",
        )

        group = self.p5["/forecast/model"]
        self.assertEqual(
            repr(group),
            "<p5netcdf.Group: /forecast/model, 3 variables, 0 sub-groups>",
        )

    def test_p5netcdf_Group__str__(self):
        """Test Group.__str__."""
        group = self.p5["/forecast"]
        self.assertEqual(
            str(group),
            """<p5netcdf.Group: /forecast, 2 variables, 1 sub-group>
Groups:
    model
Dimensions:
    lon: <p5netcdf.Dimension: /forecast/lon, size=8, unlimited>
Variables:
    lon_bnds: <p5netcdf.Variable: /forecast/lon_bnds, shape=(8, 2), dimensions=(/forecast/lon, /bounds2)>
    lon: <p5netcdf.Variable: /forecast/lon, shape=(8,), dimensions=(/forecast/lon,)>""",
        )

        group = self.p5["/forecast/model"]
        self.assertEqual(
            str(group),
            """<p5netcdf.Group: /forecast/model, 3 variables, 0 sub-groups>
Attributes:
    group_attr_1: np.int64(12)
    group_attr_2: 'bar'
Dimensions:
    lat: <p5netcdf.Dimension: /forecast/model/lat, size=5>
Variables:
    lat_bnds: <p5netcdf.Variable: /forecast/model/lat_bnds, shape=(5, 2), dimensions=(/forecast/model/lat, /bounds2)>
    lat: <p5netcdf.Variable: /forecast/model/lat, shape=(5,), dimensions=(/forecast/model/lat,)>
    q: <p5netcdf.Variable: /forecast/model/q, shape=(5, 8), dimensions=(/forecast/model/lat, /forecast/lon)>""",
        )

    def test_p5netcdf_Group__getitem__(self):
        """Test Group.__getitem__."""
        self.assertIs(self.p5[""], self.p5)
        self.assertIs(self.p5["/"], self.p5)
        self.assertIs(self.p5["//"], self.p5)
        self.assertIs(self.p5["/."], self.p5)
        self.assertIs(self.p5["./."], self.p5)
        self.assertIs(self.p5["/./."], self.p5)
        self.assertIs(self.p5["/././"], self.p5)
        self.assertIs(self.p5["forecast"], self.p5["/forecast"])
        self.assertIs(self.p5["forecast"], self.p5["/forecast/"])
        self.assertIs(self.p5["forecast/model"], self.p5["forecast"]["model"])
        self.assertIs(
            self.p5["/forecast/model/"], self.p5["/forecast"]["model/"]
        )
        self.assertIs(
            self.p5["forecast"]["/forecast/model/"], self.p5["/forecast/model"]
        )

        self.assertIs(self.p5["forecast"]["/"], self.p5["/"])
        self.assertIs(self.p5["forecast"][""], self.p5["/forecast"])
        self.assertIs(self.p5["forecast"]["/forecast"], self.p5["/forecast"])
        self.assertIs(
            self.p5["forecast"]["/forecast/model/q"],
            self.p5["/forecast/model/q"],
        )
        self.assertIs(
            self.p5["/forecast/model/q/"],
            self.p5["/forecast/model/q"],
        )
        self.assertIs(
            self.p5["/forecast//model///q////"],
            self.p5["/forecast/model/q"],
        )
        self.assertIs(
            self.p5["/forecast/model/q"],
            self.p5["/forecast"]["model"]["q"],
        )

        self.assertIs(self.p5["forecast/.."], self.p5["/"])
        self.assertIs(self.p5["/forecast/.."], self.p5["/"])
        self.assertIs(self.p5["/forecast/./.."], self.p5["/"])
        self.assertIs(self.p5["./forecast/./.."], self.p5["/"])
        self.assertIs(
            self.p5["./forecast/./../forecast/model/.."], self.p5["/forecast"]
        )

        self.assertIs(self.p5["/forecast"][""], self.p5["/forecast"])
        self.assertIs(self.p5["/forecast"]["."], self.p5["/forecast"])
        self.assertIs(self.p5["/forecast"]["./"], self.p5["/forecast"])
        self.assertIs(self.p5["/forecast"][".."], self.p5)
        self.assertIs(self.p5["/forecast"]["../"], self.p5)

        # Test bad paths from the root group
        current_group = self.p5["/"]
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
        current_group = self.p5["/forecast"]
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
        group = self.p5
        self.assertEqual(tuple(group), ("forecast", "time"))

        group = self.p5["/forecast"]
        self.assertEqual(
            tuple(group),
            (
                "model",
                "lon_bnds",
                "lon",
            ),
        )

        group = self.p5["/forecast/model"]
        self.assertEqual(tuple(group), ("lat_bnds", "lat", "q"))

    def test_p5netcdf_Group_keys(self):
        """Test Group.keys."""
        group = self.p5
        self.assertEqual(tuple(group.keys()), ("forecast", "time"))

        group = self.p5["/forecast"]
        self.assertEqual(tuple(group.keys()), ("model", "lon_bnds", "lon"))

        group = self.p5["/forecast/model"]
        self.assertEqual(tuple(group.keys()), ("lat_bnds", "lat", "q"))

    def test_p5netcdf_Group_values(self):
        """Test Group.values."""
        group = self.p5
        self.assertEqual(
            tuple(group.values()), (group["forecast"], group["time"])
        )

        group = self.p5["/forecast"]
        self.assertEqual(
            tuple(group.values()),
            (group["model"], group["lon_bnds"], group["lon"]),
        )

        group = self.p5["/forecast/model"]
        self.assertEqual(
            tuple(group.values()),
            (group["lat_bnds"], group["lat"], group["q"]),
        )

    def test_p5netcdf_Group_items(self):
        """Test Group.items."""
        group = self.p5
        self.assertEqual(
            tuple(group.items()),
            (("forecast", group["forecast"]), ("time", group["time"])),
        )

        group = self.p5["/forecast"]
        self.assertEqual(
            tuple(group.items()),
            (
                ("model", group["model"]),
                ("lon_bnds", group["lon_bnds"]),
                ("lon", group["lon"]),
            ),
        )

        group = self.p5["/forecast/model"]
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
        group = self.p5
        self.assertEqual(group.name, "")

        group = self.p5["/forecast"]
        self.assertEqual(group.name, "forecast")

        group = self.p5["/forecast/model"]
        self.assertEqual(group.name, "model")

    def test_p5netcdf_Group_path(self):
        """Test Group.path."""
        for path in ("/", "/forecast", "/forecast/model"):
            group = self.p5[path]
            self.assertEqual(group.path, path)

    def test_p5netcdf_Group_parent(self):
        """Test Group.parent."""
        group = self.p5
        self.assertIsNone(group.parent)

        group = self.p5["/forecast"]
        self.assertIs(group.parent, self.p5)

        group = self.p5["/forecast/model"]
        self.assertIs(group.parent, self.p5["/forecast"])


if __name__ == "__main__":
    print("Run date:", datetime.datetime.now())
    cfdm.environment()
    print("")
    unittest.main(verbosity=2)
