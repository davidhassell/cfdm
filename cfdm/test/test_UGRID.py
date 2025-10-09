import atexit
import datetime
import faulthandler
import itertools
import os
import tempfile
import unittest

import numpy as np

faulthandler.enable()  # to debug seg faults and timeouts

import cfdm

warnings = False

# Set up temporary files
n_tmpfiles = 2
tmpfiles = [
    tempfile.mkstemp("_test_read_write.nc", dir=os.getcwd())[1]
    for i in range(n_tmpfiles)
]
[tmpfile, tmpfile1] = tmpfiles


def _remove_tmpfiles():
    """Remove temporary files created during tests."""
    for f in tmpfiles:
        try:
            os.remove(f)
        except OSError:
            pass


atexit.register(_remove_tmpfiles)


class UGRIDTest(unittest.TestCase):
    """Test UGRID field constructs."""

    filename1 = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "ugrid_1.nc"
    )

    filename2 = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "ugrid_2.nc"
    )

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

    def test_UGRID_read(self):
        """Test reading of UGRID files."""
        f1 = cfdm.read(self.filename1)

        self.assertEqual(len(f1), 3)
        for g in f1:
            self.assertEqual(len(g.domain_topologies()), 1)
            self.assertEqual(len(g.auxiliary_coordinates()), 2)
            self.assertEqual(len(g.dimension_coordinates()), 1)

            for aux in g.auxiliary_coordinates().values():
                self.assertTrue(aux.has_data())

            if g.domain_topology().get_cell() == "face":
                self.assertEqual(len(g.cell_connectivities()), 1)
                self.assertEqual(
                    g.cell_connectivity().get_connectivity(), "edge"
                )

        f2 = cfdm.read(self.filename2)
        self.assertEqual(len(f2), 3)
        for g in f2:
            self.assertEqual(len(g.domain_topologies()), 1)
            self.assertEqual(len(g.auxiliary_coordinates()), 2)
            self.assertEqual(len(g.dimension_coordinates()), 1)

            cell = g.domain_topology().get_cell()
            if cell in ("edge", "face"):
                for aux in g.auxiliary_coordinates().values():
                    self.assertFalse(aux.has_data())

            if cell == "face":
                self.assertEqual(len(g.cell_connectivities()), 1)
                self.assertEqual(
                    g.cell_connectivity().get_connectivity(), "edge"
                )

    def test_UGRID_data(self):
        """Test reading of UGRID data."""
        node1, face1, edge1 = cfdm.read(self.filename1)
        node2, face2, edge2 = cfdm.read(self.filename2)

        # Domain topology arrays
        domain_topology1 = face1.domain_topology()
        self.assertTrue(
            (
                domain_topology1.array
                == np.array([[2, 3, 1, 0], [4, 5, 3, 2], [1, 3, 6, -99]])
            ).all()
        )
        self.assertTrue(domain_topology1.equals(face2.domain_topology()))

        domain_topology1 = edge1.domain_topology()
        self.assertTrue(
            (
                domain_topology1.array
                == np.array(
                    [
                        [1, 6],
                        [3, 6],
                        [3, 1],
                        [0, 1],
                        [2, 0],
                        [2, 3],
                        [2, 4],
                        [5, 4],
                        [3, 5],
                    ]
                )
            ).all()
        )
        self.assertTrue(domain_topology1.equals(edge2.domain_topology()))

        # Cell connectivity arrays
        cell_connectivity1 = face1.cell_connectivity()
        self.assertTrue(
            (
                cell_connectivity1.array
                == np.array(
                    [
                        [0, 1, 2, -99, -99],
                        [1, 0, -99, -99, -99],
                        [2, 0, -99, -99, -99],
                    ]
                )
            ).all()
        )
        self.assertTrue(cell_connectivity1.equals(face2.cell_connectivity()))

    def test_read_UGRID_domain(self):
        """Test reading of UGRID files into domains."""
        d1 = cfdm.read(self.filename1, domain=True)

        self.assertEqual(len(d1), 3)
        for g in d1:
            self.assertIsInstance(g, cfdm.Domain)
            self.assertEqual(len(g.domain_topologies()), 1)
            self.assertEqual(len(g.auxiliary_coordinates()), 2)
            self.assertEqual(len(g.dimension_coordinates()), 0)

            for aux in g.auxiliary_coordinates().values():
                self.assertTrue(aux.has_data())

            if g.domain_topology().get_cell() == "face":
                self.assertEqual(len(g.cell_connectivities()), 1)
                self.assertEqual(
                    g.cell_connectivity().get_connectivity(), "edge"
                )

    def test_read_write_UGRID_field(self):
        """Test the cfdm.read and cfdm.write with UGRID fields."""
        # Face, edge, and point fields that are all part of the same
        # UGRID mesh
        ugrid = cfdm.example_fields(8, 9, 10)

        face, edge, point = (0, 1, 2)

        # Test for equality with the fields defined in memory. Only
        # works for face (8) and edge (9) fields.
        for cell in (face, edge):
            f = ugrid[cell]
            cfdm.write(f, tmpfile)
            g = cfdm.read(tmpfile)
            self.assertEqual(len(g), 1)
            self.assertTrue(g[0].equals(f))

        # Test round-tripping fields with multiple fields
        #
        # Get the indices of 'ugrid' for all possible combinations of
        # fields:
        #
        # combinations = [(0,), (1,), ..., (2, 0, 1), (2, 1, 0)]
        combinations = [
            i
            for n in range(1, 4)
            for i in itertools.permutations([face, edge, point], n)
        ]

        for cells in combinations:
            f = []
            for cell in cells:
                f.append(ugrid[cell])

            cfdm.write(f, tmpfile)
            g = cfdm.read(tmpfile)
            self.assertEqual(len(g), len(f))

            cfdm.write(g, tmpfile1)
            h = cfdm.read(tmpfile1)
            self.assertEqual(len(h), len(g))
            self.assertTrue(h[0].equals(g[0]))

    def test_read_write_UGRID_domain(self):
        """Test the cfdm.read and cfdm.write with UGRID domains."""
        # Face, edge, and point fields/domains that are all part of
        # the same UGRID mesh
        ugrid = [f.domain for f in cfdm.example_fields(8, 9, 10)]

        face, edge, point = (0, 1, 2)

        # Test for equality with the fields defined in memory. Only
        # works for face (8) and edge (9) domains.
        for cell in (face, edge):
            d = ugrid[cell]
            cfdm.write(d, tmpfile)
            e = cfdm.read(tmpfile, domain=True)
            self.assertEqual(len(e), 2)
            self.assertTrue(e[0].equals(d))
            self.assertEqual(e[1].domain_topology().get_cell(), "point")

        # Test round-tripping fields with all three domains
        combinations = list(itertools.permutations([face, edge, point], 3))

        for cells in combinations:
            d = []
            for cell in cells:
                d.append(ugrid[cell])

            cfdm.write(d, tmpfile)
            e = cfdm.read(tmpfile, domain=True)
            self.assertEqual(len(e), len(d))

            cfdm.write(e, tmpfile1)
            f = cfdm.read(tmpfile1, domain=True)
            self.assertEqual(len(f), len(e))
            for i, j in zip(f, e):
                self.assertTrue(i.equals(j))

        # TODOUGRID: Other types of domain read/write are tricky, and
        #            possibly even reasonably not round-trippable, but
        #            that's an issue for another day ...


if __name__ == "__main__":
    print("Run date:", datetime.datetime.now())
    cfdm.environment()
    print("")
    unittest.main(verbosity=2)
