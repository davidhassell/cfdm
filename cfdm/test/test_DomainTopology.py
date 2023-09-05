import datetime
import faulthandler
import unittest

faulthandler.enable()  # to debug seg faults and timeouts

import cfdm

# Create test domain topology object
c = cfdm.DomainTopology()
c.set_properties({"long_name": "Maps every face to its corner nodes"})
c.nc_set_variable("Mesh2_face_nodes")
data = cfdm.Data(
    [[2, 3, 1, 0], [6, 7, 3, 2], [1, 3, 8, -99]],
    dtype="i4",
)
data.masked_values(-99, inplace=True)
c.set_data(data)
c.set_cell("face")


class DomainTopologyTest(unittest.TestCase):
    """Unit test for the DomainTopology class."""

    d = c

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

    def test_DomainTopology__repr__str__dump(self):
        """Test all means of DomainTopology inspection."""
        d = self.d
        self.assertEqual(repr(d), "<DomainTopology: cell:face(3, 4) >")
        self.assertEqual(str(d), "cell:face(3, 4) ")
        self.assertEqual(
            d.dump(display=False),
            """Domain Topology: cell:face
    long_name = 'Maps every face to its corner nodes'
    Data(3, 4) = [[2, ..., --]]""",
        )

    def test_DomainTopology_copy(self):
        """Test the copy of DomainTopology."""
        d = self.d
        self.assertTrue(d.equals(d.copy()))

    def test_DomainTopology_data(self):
        """Test the data of DomainTopology."""
        d = self.d
        self.assertEqual(d.ndim, 1)


if __name__ == "__main__":
    print("Run date:", datetime.datetime.now())
    cfdm.environment()
    print()
    unittest.main(verbosity=2)
