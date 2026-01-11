import osmnx as ox
import networkx as nx
from shapely.geometry import box
G = ox.load_graphml("dados/sp_completo.graphml")
north = -23.4799796188825
south = -23.6592587311175
east  = -46.47110851297591
west  = -46.768098674387055

bbox = box(west, south, east, north)

G_clipped = ox.truncate.truncate_graph_polygon(G, bbox)

largest_cc = max(nx.strongly_connected_components(G_clipped), key=len)
G_clipped = G_clipped.subgraph(largest_cc).copy()

ox.save_graphml(G_clipped, "sao_paulo_bbox.graphml")
ox.plot_graph(G_clipped)

