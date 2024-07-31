# standard library imports
# ...

# third party imports
import xarray as xr

# sub package imports
# ...


def wrap(ds: xr.Dataset, dim: str, vmin: float, vmax: float):
    """
    Wrap and reorder a cyclic dimension between vmin and vmax.
    The border value is duplicated at the edges.
    The period is (vmax-vmin)

    Example:
    * Dimension [0, 359] -> [-180, 180]
    * Dimension [-180, 179] -> [-180, 180]
    * Dimension [0, 359] -> [0, 360]

    Arguments:
    ----------

    ds: xarray.Dataset
    dim: str
        Name of the dimension to wrap
    vmin, vmax: float
        new values for the edges
    """

    pivot = vmax if (vmin < ds[dim][0]) else vmin

    left = ds.sel({dim: slice(None, pivot)})
    right = ds.sel({dim: slice(pivot, None)})

    if right[dim][-1] > vmax:
        # apply the offset at the right part
        right = right.assign_coords({dim: right[dim] - (vmax-vmin)})
    else:
        # apply the offset at the left part
        left = left.assign_coords({dim: left[dim] + (vmax-vmin)})

    # swaps the two parts
    return xr.concat([right, left], dim=dim)
