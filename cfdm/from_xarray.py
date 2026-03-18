from cfdm import read

def from_xarray(datasets):
    f = read(datasets)

    if len(f) == 1:
        return f[0]

    return f
