import datetime
import faulthandler
import unittest

import numpy as np

faulthandler.enable()  # to debug seg faults and timeouts

import cfdm

# Create test domain topology object
d = cfdm.DomainTopology()
d.set_property("long_name", "test")
array = np.array(
    [[0, 1, 1, 1], [1, 0, 0, 1], [1, 0, 0, 1], [1, 1, 1, 0]], dtype=bool
)
d.set_data(array)


class DomainTopologyTest(unittest.TestCase):
    """Unit test for the DomainTopology class."""

    d = d

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
        self.assertEqual(repr(d), "<DomainTopology: long_name=test(4, 4) >")
        self.assertEqual(str(d), "long_name=test(4, 4) ")
        self.assertEqual(
            d.dump(display=False),
            """Domain Topology: long_name=test
    long_name = 'test'
    Data(4, 4) = [[False, ..., False]]""",
        )

    def test_DomainTopology_copy(self):
        """Test the copy of DomainTopology."""
        d = self.d
        self.assertTrue(d.equals(d.copy()))

    def test_DomainTopology_data(self):
        """Test the data of DomainTopology."""
        d = self.d
        # Diagonal elements should be False
        self.assertTrue(not np.any(np.diagonal(d.data.array)))
        # Data should be 2-d
        self.assertEqual(d.ndim, 2)
        # Data should be symmetric
        self.assertTrue(d.data.equals(d.transpose().data))


if __name__ == "__main__":
    print("Run date:", datetime.datetime.now())
    cfdm.environment()
    print()
    unittest.main(verbosity=2)
