import datetime
import faulthandler
import unittest

faulthandler.enable()  # to debug seg faults and timeouts

import cfdm

# Create testcell connectivity object
c = cfdm.CellConnectivity()
c.set_properties({"long_name": "neighbour faces for faces"})
c.nc_set_variable("Mesh2_face_links")
data = cfdm.Data(
    [
        [0, 1, 2, -99, -99],
        [1, 0, -99, -99, -99],
        [2, 0, -99, -99, -99],
    ],
    dtype="i4",
)
data.masked_values(-99, inplace=True)
c.set_data(data)
c.set_connectivity("edge")


class CellConnectivityTest(unittest.TestCase):
    """Unit test for the CellConnectivity class."""

    c = c

    def setUp(self):
        """Preparations called immediately before each test method."""
        # Disable log messages to silence expected warnings
        cfdm.log_level("DISABLE")
        # Note: to enable all messages for given methods, lines or
        # calls (those without a 'verbose' option to do the same)
        # e.g. to debug them, wrap them (for methods, start-to-end
        # internally) as follows:
        #
        # cfdm.LOG_LEVEL('DEBUG')
        # < ... test code ... >
        # cfdm.log_level('DISABLE')

    def test_CellConnectivity__repr__str__dump(self):
        """Test all means of CellConnectivity inspection."""
        c = self.c
        self.assertEqual(
            repr(c), "<CellConnectivity: connectivity:edge(3, 5) >"
        )
        self.assertEqual(str(c), "connectivity:edge(3, 5) ")
        self.assertEqual(
            c.dump(display=False),
            """Cell Connectivity: connectivity:edge
    long_name = 'neighbour faces for faces'
    Data(3, 5) = [[0, ..., --]]""",
        )

    def test_CellConnectivity_copy(self):
        """Test the copy of CellConnectivity."""
        c = self.c
        self.assertTrue(c.equals(c.copy()))

    def test_CellConnectivity_data(self):
        """Test the data of CellConnectivity."""
        c = self.c
        self.assertEqual(c.ndim, 1)


if __name__ == "__main__":
    print("Run date:", datetime.datetime.now())
    cfdm.environment()
    print()
    unittest.main(verbosity=2)
