import datetime
import os
import tempfile
import unittest

import netCDF4
import numpy as np

import cfdm


class Testp5netcdf(unittest.TestCase):
    """Test suite for the p5netcdf read-only netCDF-4 implementation."""

    @classmethod
    def setUpClass(cls):
        """Create the test file."""
        tmpfile = tempfile.mkstemp("_test_p5netcdf.nc", dir=os.getcwd())[1]
        #        tmpfile = "test_p5_example.nc"

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

            lat = model.createVariable("lat", "f8", ("lat",))
            lat.setncattr("units", "degrees_north")
            lat.setncattr("standard_name", "latitude")
            lat.setncattr("bounds", "/forecast/model/lat_bnds")

            # The 'q' variable (uses 'lon' from the parent group and 'lat'
            # from current group)
            q = model.createVariable("q", "f4", ("lat", "lon"))

            # Complex Attributes for 'q'
            q.setncattr("int32", np.int32(49))
            q.setncattr("int64", np.int64(49))
            q.setncattr("float32", np.float32(49.0))
            q.setncattr("float64", np.float64(49.0))

            # List attributes
            q.setncattr("list1", np.array([2, 3], dtype="int64"))
            q.setncattr("list2", np.array([2, 3], dtype="int32"))
            q.setncattr("list3", np.array([2.0, 3.0], dtype="float32"))
            q.setncattr("list4", ["a", "bb", "ccc"])
            q.setncattr("list5", ["a", "1", "2.5"])

            # String attributes
            q.setncattr("string1", "1")
            q.setncattr("string2", "a")
            q.setncattr("string3", "kg m-2")
            q.setncattr("string4", "")
            q.setncattr("string5", " ")

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

        cls.test_file = tmpfile
        cls.p5 = cfdm.p5netcdf.File(tmpfile)

    @classmethod
    def tearDownClass(cls):
        """Clean up the generated test file."""
        if os.path.exists(cls.test_file):
            os.remove(cls.test_file)

    def test_p5netcdf_attributes(self):
        """Check that attributes are parsed correctly."""
        n = netCDF4.Dataset(self.test_file, "r")
        p = self.p5

        nq = n["/forecast/model/q"]
        pq = p["/forecast/model/q"]

        self.assertEqual(list(pq.attrs), nq.ncattrs())

        for attr, pvalue in pq.attrs.items():
            nvalue = nq.getncattr(attr)

            self.assertEqual(type(pvalue), type(nvalue))

            if isinstance(pvalue, np.ndarray):
                self.assertEqual(pvalue.dtype, nvalue.dtype)
                self.assertTrue(np.allclose(pvalue, nvalue))
            else:
                self.assertEqual(pvalue, nvalue)

        n.close()

    def test_p5netcdf_dimensions(self):
        """Check that dimensions are parsed correctly."""
        n = netCDF4.Dataset(self.test_file, "r")
        p = self.p5

        for group in ("/", "/forecast", "/forecast/model"):
            pg = p[group]
            ng = n if group == "/" else n[group]

            self.assertEqual(set(ng.dimensions), set(pg.dimensions))

            for name, pdim in pg.dimensions.items():
                ndim = ng.dimensions[name]
                self.assertEqual(pdim.isunlimited(), ndim.isunlimited())
                self.assertEqual(pdim.group().path, ndim.group().path)

                for name, pvar in pg.dimensions.items():
                    for attr in ("name", "size"):
                        self.assertEqual(
                            getattr(pdim, attr), getattr(ndim, attr)
                        )

        n.close()

    def test_p5netcdf_variables(self):
        """Check that variables are parsed correctly."""
        n = netCDF4.Dataset(self.test_file, "r")
        p = self.p5

        for group in ("/", "/forecast", "/forecast/model"):
            pg = p[group]
            ng = n if group == "/" else n[group]

            self.assertEqual(set(ng.variables), set(pg.variables))

            for name, pvar in pg.variables.items():
                nvar = ng.variables[name]
                self.assertEqual(pvar.chunking(), nvar.chunking())
                print(repr(pvar))
                print(pvar.get_dims())
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
        n = netCDF4.Dataset(self.test_file, "r")
        p = self.p5

        for group in ("/", "/forecast", "/forecast/model"):
            pg = p[group]
            ng = n if group == "/" else n[group]

            pattrs = pg.attrs
            nattrs = {k: ng.getncattr(k) for k in ng.ncattrs()}
            self.assertEqual(pattrs, nattrs)
            self.assertEqual(pg.name, group)
            self.assertEqual(pg.name, ng.path)

        n.close()

    def test_p5netcdf_File_filename(self):
        """Test File.filename."""
        self.assertEqual(self.p5.filename, self.test_file)

    def test_p5netcdf_Dimension_sizes(self):
        """Test Dimension.size."""
        dim = self.p5.dimensions["bounds2"]
        self.assertEqual(dim.size, 2)

    def test_p5netcdf_Dimension__len__(self):
        """Test Dimension.__len__."""
        dim = self.p5.dimensions["bounds2"]
        self.assertEqual(len(dim), dim.size)

        dim = self.p5["forecast"].dimensions["lon"]
        self.assertIsInstance(dim.isunlimited(), bool)

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

    def test_p5netcdf_Dimension_group(self):
        """Test Dimension.group."""
        dim = self.p5.dimensions["bounds2"]
        self.assertEqual(dim.group().path, "/")

        dim = self.p5["forecast"].dimensions["lon"]
        self.assertEqual(dim.group().path, "/forecast")

    def test_p5netcdf_Variable_maxshape(self):
        """Test Dimension.group."""
        var = self.p5["time"]
        self.assertEqual(var.maxshape, ())

        var = self.p5["forecast/lon"]
        self.assertEqual(var.maxshape, (None,))
        var = self.p5["forecast/lon_bnds"]

        self.assertEqual(var.maxshape, (None, 2))

    def test_p5netcdf_Group__getitem__(self):
        """Test Group.__getitem__."""
        self.assertIs(self.p5[""], self.p5)
        self.assertIs(self.p5["/"], self.p5)
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
        self.assertIs(self.p5["forecast"]["/forecast"], self.p5["/forecast"])
        self.assertIs(
            self.p5["forecast"]["/forecast/model/q"],
            self.p5["/forecast/model/q"],
        )

        for bad_group in (
            ".",
            "..",
            "/bad_group",
            "bad_group",
            "/forecast/bad_group",
            "/forecast/model/q/subgroup",
        ):
            with self.assertRaises(KeyError):
                self.p5[bad_group]


if __name__ == "__main__":
    print("Run date:", datetime.datetime.now())
    cfdm.environment()
    print("")
    unittest.main(verbosity=2)
