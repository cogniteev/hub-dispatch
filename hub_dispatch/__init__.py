import copy

__version__ = (0, 0, 1)


class GraphBackend(object):
    """ Indirect graph whose nodes are either `hubs` or `nodes`.
    It is not possible to connect nodes together.
    """
    def __init__(self, hubs=None, links=None):
        """ Create a new graph

        :param set hubs: predefined hubs
        :param dict links: nodes connection: node -> set([node, ...])
        """
        self.__hubs = hubs or set()
        self.__links = links or {}

    def add_hub(self, *hubs):
        for hub in hubs:
            if hub in self.__hubs:
                raise Exception("Hub '{}' already exists".format(hub))
            self.__hubs.add(hub)
            self.__links.setdefault(hub, set())
        return self

    def remove_hub(self, hub):
        if hub not in self.__hubs:
            self._unknown_hub(hub)
        remove_hub = True
        for node in self.__links.get(hub, set()):
            if node not in self.__hubs:
                raise Exception("Can't remove hub with connected nodes")
            elif hub in self.__links.get(node, set()):
                remove_hub = False
        self.__hubs.remove(hub)
        if remove_hub:
            self.__links.pop(hub)
        return self

    def link(self, hub, node):
        if hub == node:
            raise Exception("Hub can't be linked to itself")
        if hub not in self.__hubs:
            self._unknown_hub(hub)
        nodes = self._links(hub)
        if node in nodes:
            error_message = "Hub '{}' is already connected to node '{}'"
            raise Exception(error_message.format(hub, node))
        nodes.add(node)
        if not self.is_hub(node):
            self._links(node).add(hub)
        return self

    def unlink(self, hub, node):
        if hub not in self.__hubs:
            self._unknown_hub(hub)
        nodes = self._links(hub)
        if node not in nodes:
            error_message = "Hub '{}' is not connected to node '{}'"
            raise Exception(error_message.format(hub, node))
        if not self.is_hub(node):
            node_hubs = self._links(node)
            node_hubs.remove(hub)
            if len(node_hubs) == 0:
                self.__links.pop(node)
        nodes.remove(node)
        return self

    def _links(self, node):
        return self.__links.setdefault(node, set())

    def links(self, node):
        if node not in self.__links:
            raise Exception("Unknown node '{}'".format(node))
        return copy.deepcopy(self._links(node))

    def hub_links(self, hub):
        if not self.is_hub(hub):
            self._unknown_hub(hub)
        return self.links(hub)

    def _unknown_hub(self, hub):
        raise Exception("Hub '{}' does not exist".format(hub))

    def hubs(self):
        return copy.copy(self.__hubs)

    def is_hub(self, hub):
        return hub in self.__hubs


class TopologyBackend(object):
    def __init__(self, nodes=None, hubs=None):
        """ Typology of node assignments among available hubs
        """
        self.nodes = nodes or {}
        self.hubs = hubs or {}


class TopologyChange(object):
    def __init__(self, **kwargs):
        self._clear()

    def _clear(self):
        self.assignments = []
        self.unassignments = []

    def assign(self, hub, node):
        self.assignments.append((hub, node))

    def unassign(self, hub, node):
        self.unassignments.append((hub, node))


class HubDispatch(object):
    def __init__(self, **kwargs):
        graph_cls = kwargs.get('graph_cls', GraphBackend)
        graph_kwargs = kwargs.get('graph_kwargs', {})
        topology_cls = kwargs.get('topology_cls', TopologyBackend)
        topology_kwargs = kwargs.get('topology_kwargs', {})
        topology_change_cls = kwargs.get(
            'topology_change_cls', TopologyChange
        )
        topology_change_kwargs = kwargs.get('topology_change_kwargs', {})
        self._graph = graph_cls(**graph_kwargs)
        self._topology = topology_cls(**topology_kwargs)
        self._changes = topology_change_cls(**topology_change_kwargs)
        self._max_nodes_per_hub = kwargs.get('max_nodes_per_hub', 100)

    def add_hub(self, *hubs):
        for hub in hubs:
            self._graph.add_hub(hub)
            if hub not in self._topology.nodes:
                self._assign(hub, hub)
        return self

    def remove_hub(self, hub):
        if self._topology.nodes.get(hub) == hub:
            self._decr_hub(hub)
            self._topology.nodes.pop(hub)
            self._changes.unassign(hub, hub)
            # FIXME assign to somebody else if followed
        for node in self._graph.hub_links(hub):
            print '> unlink(%r, %r)' % (hub, node)
            self.unlink(hub, node)
        self._graph.remove_hub(hub)

    def link(self, hub, *nodes):
        for node in nodes:
            self._graph.link(hub, node)
            if not self._graph.is_hub(node):
                if node not in self._topology.nodes:
                    self._assign(node, hub)
        return self

    def unlink(self, hub, node):
        if node not in self._graph.hub_links(hub):
            error_message = "Hub '{}' is not connected to node '{}'"
            raise Exception(error_message.format(hub, node))
        other_hubs = filter(lambda h: h != hub, self._graph.links(node))
        if self._topology.nodes[node] == hub:
            if any(other_hubs):
                self._reassign(node, other_hubs, [hub])
                assert self._topology.nodes[node] != hub
            else:
                self._decr_hub(hub)
                self._topology.nodes.pop(node)
                self._changes.unassign(hub, node)
        self._graph.unlink(hub, node)
        return self

    def _decr_hub(self, hub):
        assignee = self._topology.hubs[hub]
        assert assignee > 0, "should not have less than 1 assignee"
        if assignee == 1:
            self._topology.hubs.pop(hub)
        else:
            self._topology.hubs[hub] = assignee - 1

    def _reassign(self, node, candidates, black_list=[]):
        assert any(candidates), "there should be assignment candidates"
        # find the least loaded hub among candidates
        candidate = reduce(
            self._least_loaded,
            filter(
                lambda c: c not in black_list,
                candidates
            )
        )
        candidate_assignees = self._topology.hubs.get(candidate, 0)
        if candidate_assignees >= self._max_nodes_per_hub:
            raise NotImplementedError()  # FIXME
        self._assign(node, candidate)

    def _assign(self, node, hub):
        current_hub = self._topology.nodes.get(node)
        assert current_hub != hub, 'Node is already assigned to this hub'
        assignees = self._topology.hubs.get(hub, 0)
        if assignees >= self._max_nodes_per_hub:
            raise NotImplementedError()  # FIXME
        self._topology.nodes[node] = hub
        self._topology.hubs[hub] = assignees + 1
        if current_hub is not None:
            self._decr_hub(current_hub)
            self._changes.unassign(current_hub, node)
        self._changes.assign(hub, node)

    def _least_loaded(self, c1, c2):
        c1_load = self._topology.hubs.get(c1, 0)
        c2_load = self._topology.hubs.get(c2, 0)
        if c1_load <= c2_load:
            return c1
        else:
            return c2
