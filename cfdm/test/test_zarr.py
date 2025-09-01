import atexit
import datetime
import faulthandler
import os
import shutil
import tempfile
import unittest

faulthandler.enable()  # to debug seg faults and timeouts

import zarr

import cfdm

warnings = False

# Set up temporary directories
tmpdirs = [
    tempfile.mkdtemp("_test_zarr.zarr", dir=os.getcwd()) for i in range(2)
]
[tmpdir1, tmpdir2] = tmpdirs

# Set up temporary files
tmpfiles = [
    tempfile.mkstemp("_test_zarr.nc", dir=os.getcwd())[1] for i in range(2)
]
[tmpfile1, tmpfile2] = tmpfiles


def _remove_tmpdirs():
    """Remove temporary files created during tests."""
    for f in tmpfiles:
        try:
            os.remove(f)
        except OSError:
            pass

    for d in tmpdirs:
        try:
            shutil.rmtree(d)
            os.rmdir(d)
        except OSError:
            pass


atexit.register(_remove_tmpdirs)


class read_writeTest(unittest.TestCase):
    """Test the reading and writing of field constructs from/to disk."""

    f0 = cfdm.example_field(0)

    def setUp(self):
        """Preparations called immediately before each test method."""
        # Disable log messages to silence expected warnings
        cfdm.LOG_LEVEL("DISABLE")
        # Note: to enable all messages for given methods, lines or
        # calls (those without a 'verbose' option to do the same)
        # e.g. to debug them, wrap them (for methods, start-to-end
        # internally) as follows: cfdm.LOG_LEVEL('DEBUG')
        #
        # < ... test code ... >
        # cfdm.log_level('DISABLE')

    def test_zarr_read_write_1(self):
        """Test Zarr read/write on example fields."""
        for i, f in enumerate(cfdm.example_fields()):
            if i in (8, 9, 10):
                # Can't write UGRID yet
                continue

            cfdm.write(f, tmpdir1, fmt="ZARR3")
            z = cfdm.read(tmpdir1)
            self.assertEqual(len(z), 1)
            z = z[0]
            self.assertTrue(z.equals(f))

            # Check that the Zarr and netCDF4 encodings are equivalent
            cfdm.write(f, tmpfile1, fmt="NETCDF4")
            n = cfdm.read(tmpfile1)[0]
            self.assertTrue(z.equals(n))

    def test_zarr_read_write_2(self):
        """Test Zarr read/write on various netCDF files."""
        for filename in (
            "DSG_timeSeries_contiguous.nc",
            "DSG_timeSeries_indexed.nc",
            "DSG_timeSeriesProfile_indexed_contiguous.nc",
            "gathered.nc",
            "geometry_1.nc",
            "geometry_2.nc",
            "geometry_3.nc",
            "geometry_4.nc",
            "string_char.nc",
        ):
            n = cfdm.read(filename)
            cfdm.write(n, tmpdir1, fmt="ZARR3")
            z = cfdm.read(tmpdir1)
            self.assertEqual(len(z), len(n))
            for a, b in zip(z, n):
                self.assertTrue(a.equals(b))

    def test_zarr_read_write_chunks_shards(self):
        """Test Zarr read/write with chunks and shards."""
        f = self.f0.copy()
        f.data.nc_set_dataset_chunksizes([2, 3])

        cfdm.write(f, tmpdir1, fmt="ZARR3")
        z = cfdm.read(tmpdir1)[0]
        self.assertTrue(z.equals(f))

        z = zarr.open(tmpdir1)
        self.assertEqual(z["q"].chunks, (2, 3))
        self.assertIsNone(z["q"].shards)

        # Make shards comprising 4 chunks
        cfdm.write(f, tmpdir1, fmt="ZARR3", dataset_shards=4)
        z = cfdm.read(tmpdir1, store_dataset_shards=False)[0]
        self.assertTrue(z.equals(f))
        self.assertIsNone(z.data.nc_dataset_shards())

        z = zarr.open(tmpdir1)
        self.assertEqual(z["q"].chunks, (2, 3))
        self.assertEqual(z["q"].shards, (4, 6))

        for shards in (4, [2, 2]):
            f.data.nc_set_dataset_shards(shards)
            cfdm.write(f, tmpdir1, fmt="ZARR3")
            z = cfdm.read(tmpdir1)[0]
            self.assertTrue(z.equals(f))
            self.assertEqual(z.data.nc_dataset_shards(), (2, 2))

            z = zarr.open(tmpdir1)
            self.assertEqual(z["q"].chunks, (2, 3))
            self.assertEqual(z["q"].shards, (4, 6))

    def test_zarr_read_write_CFA(self):
        """Test CF aggreagtion in Zarr."""
        f = self.f0

        cfdm.write(f, tmpdir1, fmt="ZARR3")
        cfdm.write(f, tmpfile1, fmt="NETCDF4")

        z = cfdm.read(tmpdir1, cfa_write="field")[0]
        n = cfdm.read(tmpfile1, cfa_write="field")[0]

        self.assertTrue(z.equals(f))
        self.assertTrue(z.equals(n))

        cfdm.write(z, tmpdir2, fmt="ZARR3", cfa="field")
        cfdm.write(n, tmpfile2, fmt="NETCDF4", cfa="field")

        z = cfdm.read(tmpdir2)[0]
        n = cfdm.read(tmpfile2)[0]

        self.assertTrue(z.equals(f))
        self.assertTrue(z.equals(n))

    def test_zarr_groups_1(self):
        """Test for the general handling of Zarr hierarchical groups."""
        f = cfdm.example_field(1)

        # Add a second grid mapping
        datum = cfdm.Datum(parameters={"earth_radius": 7000000})
        conversion = cfdm.CoordinateConversion(
            parameters={"grid_mapping_name": "latitude_longitude"}
        )

        grid = cfdm.CoordinateReference(
            coordinate_conversion=conversion,
            datum=datum,
            coordinates=["auxiliarycoordinate0", "auxiliarycoordinate1"],
        )

        f.set_construct(grid)

        grid0 = f.construct("grid_mapping_name:rotated_latitude_longitude")
        grid0.del_coordinate("auxiliarycoordinate0")
        grid0.del_coordinate("auxiliarycoordinate1")

        grouped_dir = tmpdir1
        grouped_file = tmpfile1

        # Set some groups
        f.nc_set_variable_groups(["forecast", "model"])
        f.construct("grid_latitude").bounds.nc_set_variable_groups(
            ["forecast"]
        )
        for name in (
            "longitude",  # Auxiliary coordinate
            "latitude",  # Auxiliary coordinate
            "long_name=Grid latitude name",  # Auxiliary coordinate
            "measure:area",  # Cell measure
            "surface_altitude",  # Domain ancillary
            "air_temperature standard_error",  # Field ancillary
            "grid_mapping_name:rotated_latitude_longitude",
            "time",  # Dimension coordinate
            "grid_latitude",  # Dimension coordinate
        ):
            f.construct(name).nc_set_variable_groups(["forecast"])

        # Check the groups
        cfdm.write(f, grouped_file, fmt="NETCDF4")
        cfdm.write(f, grouped_dir, fmt="ZARR3")

        n = cfdm.read(grouped_file)[0]
        z = cfdm.read(grouped_dir)[0]
        self.assertTrue(z.equals(n))
        self.assertTrue(z.equals(f))

        # Directly check the groups in the Zarr dataset
        x = zarr.open(grouped_dir)
        self.assertEqual(list(x.group_keys()), ["forecast"])
        self.assertEqual(list(x["forecast"].group_keys()), ["model"])

        cfdm.write(z, tmpdir2, fmt="ZARR3")
        z1 = cfdm.read(tmpdir2)[0]
        self.assertTrue(z1.equals(f))

    def test_zarr_groups_dimension(self):
        """Test the dimensions of Zarr hierarchical groups."""
        f = self.f0.copy()

        grouped_dir = tmpdir1
        grouped_file = tmpfile1

        # Set some groups
        f.nc_set_variable_groups(["forecast", "model"])
        for construct in f.constructs.filter_by_data().values():
            construct.nc_set_variable_groups(["forecast"])

        for construct in f.coordinates().values():
            try:
                construct.bounds.nc_set_variable_groups(["forecast"])
            except ValueError:
                pass

        domain_axis = f.domain_axis("latitude")
        domain_axis.nc_set_dimension_groups(["forecast"])

        # Check the groups
        cfdm.write(f, grouped_file, fmt="NETCDF4")
        cfdm.write(f, grouped_dir, fmt="ZARR3")

        n = cfdm.read(grouped_file)[0]
        z = cfdm.read(grouped_dir)[0]
        self.assertTrue(z.equals(n))
        self.assertTrue(z.equals(f))

    def test_zarr_groups_compression(self):
        """Test the compression of Zarr hierarchical groups."""
        f = cfdm.example_field(4)

        grouped_dir = "tmpdir1"
        grouped_file = "tmpfile1.nc"

        f.compress("indexed_contiguous", inplace=True)
        f.data.get_count().nc_set_variable("count")
        f.data.get_index().nc_set_variable("index")

        # Set some groups. (Write the read the field first to create
        # the compressions variables on disk.)
        cfdm.write(f, tmpfile2)
        f = cfdm.read(tmpfile2)[0]

        # Set some groups
        f.nc_set_variable_groups(["forecast", "model"])
        f.data.get_count().nc_set_variable_groups(["forecast"])
        f.data.get_index().nc_set_variable_groups(["forecast"])
        f.construct("altitude").nc_set_variable_groups(["forecast"])
        f.data.get_count().nc_set_sample_dimension_groups(["forecast"])

        cfdm.write(f, grouped_file, fmt="NETCDF4")
        cfdm.write(f, grouped_dir, fmt="ZARR3")

        n = cfdm.read(grouped_file)
        z = cfdm.read(grouped_dir)

        n = n[0]
        z = z[0]
        self.assertTrue(z.equals(n))
        self.assertTrue(z.equals(f))

    def test_zarr_groups_geometry(self):
        """Test that geometries in Zarr groups."""
        f = cfdm.example_field(6)

        grouped_dir = tmpdir1
        grouped_file = tmpfile1

        cfdm.write(f, tmpfile2)
        f = cfdm.read(tmpfile2)[0]

        # Set some groups
        f.nc_set_variable_groups(["forecast", "model"])
        f.nc_set_geometry_variable_groups(["forecast"])
        f.coordinate("longitude").bounds.nc_set_variable_groups(["forecast"])
        f.nc_set_component_variable_groups("node_count", ["forecast"])
        f.nc_set_component_variable_groups("part_node_count", ["forecast"])
        f.nc_set_component_variable("interior_ring", "interior_ring")
        f.nc_set_component_variable_groups("interior_ring", ["forecast"])

        # Check the groups
        cfdm.write(f, grouped_file, fmt="NETCDF4")
        cfdm.write(f, grouped_dir, fmt="ZARR3")

        n = cfdm.read(grouped_file)[0]
        z = cfdm.read(grouped_dir)[0]
        self.assertTrue(z.equals(n))
        self.assertTrue(z.equals(f))


if __name__ == "__main__":
    print("Run date:", datetime.datetime.now())
    cfdm.environment()
    print("")
    unittest.main(verbosity=2)
