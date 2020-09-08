import datetime
import inspect
import unittest

import cfdm


class DocstringTest(unittest.TestCase):
    def setUp(self):
        self.package = 'cfdm'
        self.repr = ''

        self.subclasses_of_Container = (
            cfdm.Field,
            cfdm.AuxiliaryCoordinate,
            cfdm.DimensionCoordinate,
            cfdm.DomainAncillary,
            cfdm.FieldAncillary,
            cfdm.CellMeasure,
            cfdm.DomainAxis,
            cfdm.CoordinateReference,
            cfdm.CellMethod,

            cfdm.core.Field,
            cfdm.core.AuxiliaryCoordinate,
            cfdm.core.DimensionCoordinate,
            cfdm.core.DomainAncillary,
            cfdm.core.FieldAncillary,
            cfdm.core.CellMeasure,
            cfdm.core.DomainAxis,
            cfdm.core.CoordinateReference,
            cfdm.core.CellMethod,

            cfdm.NodeCountProperties,
            cfdm.PartNodeCountProperties,
            cfdm.Bounds,
            cfdm.InteriorRing,
            cfdm.List,
            cfdm.Index,
            cfdm.Count,

            cfdm.Data,
            cfdm.NetCDFArray,
            cfdm.NumpyArray,
            cfdm.GatheredArray,
            cfdm.RaggedContiguousArray,
            cfdm.RaggedIndexedArray,
            cfdm.RaggedIndexedContiguousArray,

            cfdm.Constructs,

            cfdm.core.Constructs,

            cfdm.core.abstract.Properties,
            cfdm.core.abstract.PropertiesData,
            cfdm.core.abstract.PropertiesDataBounds,
            cfdm.core.abstract.Coordinate,

        )
        self.subclasses_of_Properties = (
            cfdm.Field,
            cfdm.AuxiliaryCoordinate,
            cfdm.DimensionCoordinate,
            cfdm.DomainAncillary,
            cfdm.FieldAncillary,
            cfdm.CellMeasure,
            cfdm.NodeCountProperties,
            cfdm.PartNodeCountProperties,
            cfdm.Bounds,
            cfdm.InteriorRing,
            cfdm.List,
            cfdm.Index,
            cfdm.Count,
        )
        self.subclasses_of_PropertiesData = (
            cfdm.Field,
            cfdm.AuxiliaryCoordinate,
            cfdm.DimensionCoordinate,
            cfdm.DomainAncillary,
            cfdm.FieldAncillary,
            cfdm.CellMeasure,
            cfdm.Bounds,
            cfdm.InteriorRing,
            cfdm.List,
            cfdm.Index,
            cfdm.Count,
        )
        self.subclasses_of_PropertiesDataBounds = (
            cfdm.AuxiliaryCoordinate,
            cfdm.DimensionCoordinate,
            cfdm.DomainAncillary,
        )

    def test_docstring(self):
        # Test that all {{ occurences have been substituted
        for klass in self.subclasses_of_Container:
            for x in (klass, klass()):
                for name in dir(x):
                    f = getattr(klass, name, None)

                    if f is None or not hasattr(f, '__doc__'):
                        continue

                    if name.startswith('__') and not inspect.isfunction(f):
                        continue

                    self.assertIsNotNone(
                        f.__doc__,
                        "\n\nCLASS: {}\n"
                        "METHOD NAME: {}\n"
                        "METHOD: {}\n__doc__: {}".format(
                            klass, name, f, f.__doc__))

                    self.assertNotIn(
                        '{{', f.__doc__,
                        "\n\nCLASS: {}\n"
                        "METHOD NAME: {}\n"
                        "METHOD: {}".format(
                            klass, name, f))

    def test_docstring_package(self):
        string = '>>> f = {}.'.format(self.package)
        for klass in self.subclasses_of_Container:
            for x in (klass, klass()):
                self.assertIn(string, x._has_component.__doc__, klass)

        string = '>>> f = {}.'.format(self.package)
        for klass in self.subclasses_of_Properties:
            for x in (klass, klass()):
                self.assertIn(string, x.clear_properties.__doc__, klass)

    def test_docstring_class(self):
        for klass in self.subclasses_of_Properties:
            string = '>>> f = {}.{}'.format(self.package, klass.__name__)
            for x in (klass, klass()):
                self.assertIn(
                    string, x.clear_properties.__doc__,
                    "\n\nCLASS: {}\n"
                    "METHOD NAME: {}\n"
                    "METHOD: {}".format(
                        klass, 'clear_properties', x.clear_properties))

        for klass in self.subclasses_of_Container:
            string = klass.__name__
            for x in (klass, klass()):
                self.assertIn(string, x.copy.__doc__, klass)

        for klass in self.subclasses_of_PropertiesDataBounds:
            string = '{}'.format(klass.__name__)
            for x in (klass, klass()):
                self.assertIn(
                    string, x.insert_dimension.__doc__,
                    "\n\nCLASS: {}\n"
                    "METHOD NAME: {}\n"
                    "METHOD: {}".format(
                        klass, klass.__name__, 'insert_dimension'))

    def test_docstring_repr(self):
        string = '<{}Data'.format(self.repr)
        for klass in self.subclasses_of_PropertiesData:
            for x in (klass, klass()):
                self.assertIn(string, x.has_data.__doc__, klass)

    def test_docstring_default(self):
        string = 'Return the value of the *default* parameter'
        for klass in self.subclasses_of_Properties:
            for x in (klass, klass()):
                self.assertIn(string, x.del_property.__doc__, klass)

    def test_docstring_staticmethod(self):
        string = 'Return the value of the *default* parameter'
        for klass in self.subclasses_of_PropertiesData:
            x = klass
            self.assertEqual(
                x._test_docstring_substitution_staticmethod(1, 2),
                (1, 2)
            )

    def test_docstring_classmethod(self):
        string = 'Return the value of the *default* parameter'
        for klass in self.subclasses_of_PropertiesData:
            for x in (klass, klass()):
                self.assertEqual(
                    x._test_docstring_substitution_classmethod(1, 2),
                    (1, 2)
                )

    def test_docstring_docstring_substitutions(self):
        for klass in self.subclasses_of_Container:
            for x in (klass,):
                d = x._docstring_substitutions(klass)
                self.assertIsInstance(d, dict)
                self.assertIn('{{repr}}', d)

# --- End: class


if __name__ == '__main__':
    print('Run date:', datetime.datetime.now())
    cfdm.environment()
    print('')
    unittest.main(verbosity=2)