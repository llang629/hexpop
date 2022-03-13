"""Demo test of H3 functions."""
import os

import h3

HOME_HEX = os.getenv('HOME_HEX')
print(HOME_HEX)
home_latlon = h3.h3_to_geo(HOME_HEX)
home_children = h3.h3_to_children(HOME_HEX)
print("There's no place like home...")
print(f"   Lat/Lon:     {home_latlon}")
print(f"   H3 Index:    {HOME_HEX}")
print(f"   H3 Children: {home_children}")
