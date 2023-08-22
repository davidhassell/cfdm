"""Define docstring substitutions.

Text to be replaced is specified as a key in the returned dictionary,
with the replacement text defined by the corresponding value.

Special docstring substitutions, as defined by a class's
`_docstring_special_substitutions` method, may be used in the
replacement text, and will be substituted as usual.

Replacement text may not contain other non-special substitutions.

Keys must be `str` or `re.Pattern` objects:

* If a key is a `str` then the corresponding value must be a string.

* If a key is a `re.Pattern` object then the corresponding value must
  be a string or a callable, as accepted by the `re.Pattern.sub`
  method.

.. versionaddedd:: (cfdm) 1.8.7.0

"""
_docstring_substitution_definitions = {
    # ----------------------------------------------------------------
    # General susbstitutions (not indent-dependent)
    # ----------------------------------------------------------------
    "{{repr}}": "",
    # ----------------------------------------------------------------
    # # Method description susbstitutions (1 level of indentation)
    # ----------------------------------------------------------------
    # cell type
    "{{cell type}}": """The cell type indicates the common geometrical element type of
        the cells for which there is a domain topology. It takes one
        of the following values: TODOUGRID

        * ``'point'``: Node cells comprising 0-dimensional elements
                       each defined by a point. TODOUGRID

        * ``'edge'``: Edge cells comprising 1-dimensional elements
                      each defined by a line bounded by two nodes. TODOUGRID

        * ``'face'``: Face cells comprising 2-dimensional elements TODOUGRID
                      each defined by a surface enclosed by a set of edges.

        * ``'volume'``: Volume cells comprising 3-dimensional elements
                        each defined by a volume enclosed by a set of
                        faces. TODOUGRID""",
    # cell connectivity type
    "{{cell connectivity type}}": """The connectivity type describes a characteristic of inter-cell
        connectivity defined by the domain topology construct. It may
        take any value, but the following values are standardised:

        * ``'node'``: Edge, face or volume cells connected by one or
                      more shared nodes.

        * ``'edge'``: Face or volume cells connected by one or more
                      shared edges.

        * ``'face'``: Volume cells connected by one or more shared
                      faces.""",
    # ----------------------------------------------------------------
    # # Method description susbstitutions (2 levels of indentation)
    # ----------------------------------------------------------------
    # cached: optional
    "{{cached: optional}}": """cached: optional
                If any value other than `None` then return *cached*
                without selecting any constructs.""",
    # todict: `bool`, optional
    "{{todict: `bool`, optional}}": """todict: `bool`, optional
                If True then return a dictionary of constructs keyed
                by their construct identifiers, instead of a
                `Constructs` object. This is a faster option.""",
    # ----------------------------------------------------------------
    # # Method description susbstitutions (3 levels of indentation)
    # ----------------------------------------------------------------
    # axes int examples
    "{{axes int examples}}": """Each axis is identified by its integer position in the
                data. Negative integers counting from the last
                position are allowed.

                *Parameter example:*
                  ``axes=0``

                *Parameter example:*
                  ``axes=-1``

                *Parameter example:*
                  ``axes=[1, -2]``""",
    # default Exception
    "{{default Exception}}": """If set to an `Exception` instance then it will be
                raised instead.""",
    # inplace: `bool`, optional (default True)
    "{{inplace: `bool`, optional (default True)}}": """inplace: `bool`, optional:
                If True (the default) then do the operation in-place and
                return `None`. If False a new, modified `{{class}}`
                instance is returned.""",
    # init properties
    "{{init properties: `dict`, optional}}": """properties: `dict`, optional
                Set descriptive properties. The dictionary keys are
                property names, with corresponding values.

                Properties may also be set after initialisation with
                the `set_properties` and `set_property` methods.""",
    # init data
    "{{init data: data_like, optional}}": """data: data_like, optional
                Set the data.

                {{data_like}}

                The data also may be set after initialisation with the
                `set_data` method.""",
    # init bounds
    "{{init bounds: `Bounds`, optional}}": """bounds: `Bounds`, optional
                Set the bounds array.

                The bounds array may also be set after initialisation
                with the `set_bounds` method.""",
    # init geometry
    "{{init geometry: `str`, optional}}": """geometry: `str`, optional
                Set the geometry type.

                The geometry type may also be set after initialisation
                with the `set_geometry` method.

                *Parameter example:*
                  ``geometry='polygon'``""",
    # init interior_ring
    "{{init interior_ring: `InteriorRing`, optional}}": """interior_ring: `InteriorRing`, optional
                Set the interior ring variable.

                The interior ring variable may also be set after
                initialisation with the `set_interior_ring` method.""",
    # init copy
    "{{init copy: `bool`, optional}}": """copy: `bool`, optional
                If True (the default) then deep copy the input
                parameters prior to initialisation. By default the
                parameters are not deep copied.""",
    # init source
    "{{init source: optional}}": """source: optional
                Convert *source*, which can be any type of object, to
                a `{{class}}` instance.

                All other parameters, apart from *copy*, are ignored
                and their values are instead inferred from *source* by
                assuming that it has the `{{class}}` API. Any
                parameters that can not be retrieved from *source* in
                this way are assumed to have their default value.

                Note that if ``x`` is also a `{{class}}` instance then
                ``{{package}}.{{class}}(source=x)`` is equivalent to
                ``x.copy()``.""",
    # data_like
    "{{data_like}}": """A data_like object is any object that can be converted
                to a `Data` object, i.e. `numpy` array_like objects,
                `Data` objects, and {{package}} instances that contain
                `Data` objects.""",
    # data: `bool`, optional
    "{{data: `bool`, optional}}": """data: `bool`, optional
                If True (the default) then copy data contained in the
                metadata construct(s), else the data is not copied.""",
    # (component-based) copy: `bool`, optional
    "{{copy: `bool`, optional}}": """copy: `bool`, optional
                If True (the default) then copy the component prior to
                insertion, else it is not copied.""",
    # data copy: `bool`, optional
    "{{data copy: `bool`, optional}}": """copy: `bool`, optional
                If True (the default) then copy the data prior to
                insertion, else the data is not copied.""",
}
