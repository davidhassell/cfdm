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
        """Create test file."""
        f = cfdm.example_field(0)

        # Attributes
        f.clear_properties()
        f.set_properties(
            {
                "int": 49,
                "float": 49.0,
                "int32": np.int32(49),
                "int64": np.int64(49),
                "float32": np.float32(49.0),
                "float64": np.float64(49.0),
                "list1": [2, 3],
                "list2": np.array([2, 3], dtype="int32"),
                "list3": np.array([2, 3], dtype="float32"),
                "list4": ["a", "bb", "ccc"],
                "list5": ["a", 1, 2.5],
                "string1": "1",
                "string2": "a",
                "string3": "kg m-2",
                "string4": "",
                "string5": " ",
                "group_attr_1": 12,
                "group_attr_2": "foo",
            }
        )

        # Groups
        f.nc_set_variable_groups(["forecast", "model"])
        f.coordinate("latitude").nc_set_variable_groups(["forecast", "model"])
        f.coordinate("longitude").nc_set_variable_groups(["forecast"])

        # Group attributes
        f.nc_set_group_attributes({"group_attr_1": None, "group_attr_2": None})

        # Unlimited dimensions
        f.domain_axis("longitude").nc_set_unlimited(True)

        tmpfile = tempfile.mkstemp("_test_p5netcdf.nc", dir=os.getcwd())[1]
        cfdm.write(f, tmpfile, fmt="NETCDF4", netcdf_backend="netCDF4")

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
        """Test basic properties of the File root object."""
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
