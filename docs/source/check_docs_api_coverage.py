"""Check the method-coverage of all classes in docs/source/class.rst.

All non-private methods of all such classes are checked for having an
entry in their corresponding class's file in docs/source/class/

For example, all cfdm.Field methods that do not start with an
underscore are checked for having an entry in
docs/source/class/cfdm.Field.rst

If being used on the cfdm library, the classes listed in
docs/source/class_core.rst are also checked.

Call as:

   $ python check_api_coverage.py <docs/source/ directory path>

"""
import os
import re
import sys

import cfdm as package

if len(sys.argv) == 2:
    source = sys.argv[1]
else:
    raise ValueError(
        "Must provide the 'source' directory as the "
        "only positional argument"
    )


if not source.endswith("source"):
    raise ValueError(f"Given directory {source} does not end with 'source'")

n_undocumented_methods = 0
n_missing_files = 0
duplicate_method_entries = []

for core in ("", "_core"):
    if core:
        if package.__name__ != "cfdm":
            # Only check core methods on cfdm package
            continue

        package = getattr(package, "core")

    with open(os.path.join(source, "class" + core + ".rst")) as f:
        api_contents = f.read()

    class_names = [
        i.split(".")[-1]
        for i in api_contents.split("\n")
        if package.__name__ + "." in i
    ]

    for class_name in class_names:
        klass = getattr(package, class_name)
        methods = [
            method for method in dir(klass) if not method.startswith("_")
        ]

        class_name = ".".join([package.__name__, class_name])

        rst_file = os.path.join(source, "class", class_name + ".rst")

        try:
            with open(rst_file) as f:
                rst_contents = f.read()

            for method in methods:
                method = ".".join([class_name, method])
                count = rst_contents.count(method)
                if count == 0:
                    n_undocumented_methods += 1
                    print(
                        f"Method {method} not in "
                        f"{os.path.join(source, 'class', rst_file)}"
                    )
                elif count > 1:
                    # The method appears more than once, but may be a
                    # sub-string of another method name, e.g. this gets caught:
                    #     [cfdm.List.]nc_set_variable
                    # due to the presence of this method:
                    #     [cfdm.List.]nc_set_variable_groups
                    # so we must account for that. Checking next character
                    # of duplicate(s) is not an underscore, is easiest.
                    locs = [
                        (m.start(0), m.end(0))
                        for m in re.finditer(method, rst_contents)
                    ]
                    chars_at_next_loc = [
                        m[1] for m in [lcs for lcs in locs]  # == last char + 1
                    ]
                    chars = [rst_contents[c] for c in chars_at_next_loc]

                    newline_count = chars.count("\n")
                    ws_count = chars.count(" ")
                    # Any character that isn't a newline or whitespace
                    # indicates another method which the method is a substring
                    # of and can be excluded. If there are still duplicates,
                    # we have genuine duplicate listing entries to report.
                    if newline_count + ws_count > 1:
                        duplicate_method_entries.append(method)
        except FileNotFoundError:
            n_missing_files += 1
            print(f"File {rst_file} does not exist")

# Raise an exception to ensure a non-zero shell return code
if n_undocumented_methods:
    print("Found undocumented method(s)")

if n_missing_files:
    print("Found missing .rst file(s)")

if n_undocumented_methods or n_missing_files:
    raise ValueError(
        f"Found undocumented methods ({n_undocumented_methods}) "
        f"or missing .rst files ({n_missing_files})"
    )

if duplicate_method_entries:
    duplicate_method_entries.sort()
    entries = "\n".join(duplicate_method_entries)  # can't set \n in f-string!
    print(
        "WARNING: there are methods which are listed multiple times "
        "in one class file/page. Decide if the duplicates are useful. "
        f"They are:\n{entries}\n\n"
    )

print("All non-private methods are documented")
