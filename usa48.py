"""Creates mask and plots map of USA lower 48 states."""
import geopandas
import matplotlib.pyplot as plt

USA48_LIST = """
AL,AR,AZ,CA,CO,CT,DC,DE,FL,GA,IA,ID,IL,IN,KS,KY,LA,MA,MD,ME,MI,MN,MO,MS,MT,\
NC,ND,NE,NH,NJ,NM,NV,NY,OH,OK,OR,PA,RI,SC,SD,TN,TX,UT,VA,VT,WA,WI,WV,WY
"""
print(USA48_LIST)

world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
usa = world[world['name'] == 'United States of America']
usa48 = usa.explode(ignore_index=True).drop(range(1, 10))  # drop AK and HI
usa48.plot()
plt.show()
