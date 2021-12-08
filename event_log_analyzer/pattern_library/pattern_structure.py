"""
This module contains the functionality that organizes the pattern checking process by structuring them in a graph
"""
import os
import networkx as nx
import matplotlib.pyplot as plt
from event_log_analyzer.utils import logger, log_time, pattern_logger
from event_log_analyzer.pattern_library.manufacturing_scheduling_patterns import DistinguishableResource, FlowShop, IndistinguishableResource, JobShop, ManufacturingScheduling, NoWait, OneBlocking, Permutation, ResourceSetupTimes 
 
class PatternStructure():
    """
    A PatternStructure object contains all patterns in a structured way (an aycyclic directed graph), so that the event log can be checked efficiently pattern by pattern.
    
    Attributes
    -----------
    dependency_graph : networkx.DiGraph
        a networkx directed graph object in which the pattern objects are stored
        
    topological_order : List[Pattern]
        a list of all patterns in a topological order
    """
    def __init__(self):
        """initialize the PatternStructure
        """
        logger.info("Set up Pattern Structure")
                
        self.dependency_graph = nx.DiGraph() #Node object  
              
        pattern_list = [ManufacturingScheduling(),
                        Permutation(), 
                        JobShop(), 
                        FlowShop(), 
                        DistinguishableResource(), 
                        IndistinguishableResource(), 
                        ResourceSetupTimes(), 
                        NoWait(), 
                        OneBlocking()]
        self.dependency_graph.add_nodes_from(pattern_list)
        
        for pattern in self.dependency_graph:
            pattern.add_dependencies(self)
            
        self.topological_ordering()
            
    @log_time(logger,"pattern check duration")
    def check_all_patterns(self, event_log):
        """
        check all patterns that are initialized in the pattern structure in a topological order and log whether they apply
        
        Arguments
        -----------
        log : EventLogStorage
            the event log on which the patterns should be classified
        """        
        for p in self.topological_order:
            edges = self.dependency_graph.out_edges(p, data="type")
            if p.pattern_applies(event_log):
                p.applies = True
                pattern_logger.info(f"✅ \tThe {p.name} applies!")
                for e in edges:
                    if e[2]=="forces":
                        pattern_logger.info(f"\t--> forces {e[1].name}")
                        e[1].applies = True
            else:
                p.applies = False
                pattern_logger.info(f"❌ \tThe {p.name} does not apply!")
                for e in edges:
                    if e[2]=="enables":
                        pattern_logger.info(f"\t--> excludes {e[1].name}")
                        e[1].applies = False   
                        
    def applying_pattern_list(self):
        """
        returns a list of all patterns that apply

        Returns
        -------
        pattern_list: List[Pattern] 
            list of all patterns that apply
            
        Raises
        ------
        ValueError
            If the patterns have not been checked yet.
        """
        pattern_list = []
        for p in self.topological_order:
            if p.applies:
                pattern_list.append(p)
            elif p.applies is None:
                raise ValueError(f"Not all patterns have been checked yet! (for example:{p.name})")
        return pattern_list
                
    
    def plot_pattern_structure(self, file='pattern_structure.pdf'):   
        """
        plots the pattern structure in the given file as a matplotlib figure
        
        Arguments
        ----------
        file : str, optional
            The file name of the output where the graphic should be plotted (default is pattern_structure.pdf)
        """ 
        plt.figure(figsize=(20, 10))    
        labels = {}
        for p in self.dependency_graph.nodes:
            labels[p] = p.name             
        try: 
            from networkx.drawing.nx_agraph import graphviz_layout
            pos = graphviz_layout(self.dependency_graph, prog='dot')
            nx.draw(self.dependency_graph, pos, labels=labels, with_labels=True, node_size=1500, node_color="#ffffff")
            
            edge_labels = nx.get_edge_attributes(self.dependency_graph,'type')
            enables_edges = {k: v for k, v in edge_labels.items() if v=="enables"}
            nx.draw_networkx_edge_labels(self.dependency_graph, pos, edge_labels=enables_edges, font_color="r")
            force_edges = {k: v for k, v in edge_labels.items() if v=="forces"}
            nx.draw_networkx_edge_labels(self.dependency_graph, pos, edge_labels=force_edges, font_color="grey")
            
            plt.savefig(f'{os.getcwd()}/output/{file}', bbox_inches='tight')   
        except:
            raise ImportError("requires pygraphviz, this package requires additional dependencies (see http://pygraphviz.github.io/), therefore we decided to not install it by default (on MacOS use: brew install graphviz & pip install pygraphviz)")
          
    def topological_ordering(self):
        """
        finds the topological ordering of the initialized pattern structure and stores it in the topological_order attribute
        """
        graph_only_with_preconditions = nx.DiGraph(((u, v, e) for u,v,e in self.dependency_graph.edges(data=True) if e["type"] == 'enables'))
        self.topological_order = list(nx.topological_sort(graph_only_with_preconditions))
        for p in self.dependency_graph.nodes:
            if p not in self.topological_order:
                self.topological_order.insert(0, p)
    
    def print_topological_ordering(self):
        """
        prints the topological order in a list object
        """
        print("Topological Order:")
        print(list(map(lambda p: p.name, self.topological_order)))