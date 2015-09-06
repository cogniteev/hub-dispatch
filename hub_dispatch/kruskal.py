
# def kruskall(g):
#     hubs = g.hubs()
#     all_hubs = g.hubs()
#     cfcs = []
#     while any(hubs):
#         cfc = GraphBackend()
#         cfc_hubs = [hubs.pop()]
#         while any(cfc_hubs):
#             hub = cfc_hubs.pop()
#             cfc.add_hub(hub)
#             for node in cfc.links(hub):
#                 if node in all_hubs:
#                     cfc.add_hub(node)
#                     hubs.remove(hub)
#                     cfc_hubs.add(hub)
#                 cfc.link(hub, node)
#         cfcs.append(cfc)
#     return cfcs
