"""
This module implements contains the abstract pattern class that must be extended by any pattern.
"""
from abc import ABC, abstractmethod
from event_log_analyzer.utils import pattern_logger

class Pattern(ABC):        
    """
    The abstract Pattern class describes single constraint patterns with all its dependencies. 
    
    Attributes
    -----------
    name : str
        a unique name, with which each pattern can be identified
    
    dependencies : Dict[str, List[str]]
        dependencies to other patterns (identified by their name) given as a key value pair where the key stands for the type of dependency (for example necessary conditions)
    
    applies : bool
        True if the pattern applies, False if not, None if it has not been checked yet        
    """
    def __init__(self):
        self.applies = None
    
    def add_dependencies(self, pattern_structure):
        """
        adds all dependencies to other patterns in the pattern structure 
        
        Arguments
        -----------
        pattern_structure : PatternStructure
            the Pattern Structure object in which the dependencies should be linked
        """
        for d in self.dependencies["requires"]:
            node_list = [x for x,y in pattern_structure.dependency_graph.nodes(data=True) if x.name==d]
            node = node_list[0]
            pattern_structure.dependency_graph.add_edge(node, self, type="enables") #in the graph the requires edge (A requires B) is rewritten as a enables edge (B enables A), both is equivalent
        
        for d in self.dependencies["forces"]:
            node_list = [x for x,y in pattern_structure.dependency_graph.nodes(data=True) if x.name==d]
            node = node_list[0]    
            pattern_structure.dependency_graph.add_edge(self, node, type="forces")
                    
    def print_name(self):
        """
        prints the unique name
        """
        print(self.name)
    
    @property
    @abstractmethod
    def dependencies(self):
        pass

    @property
    @abstractmethod
    def name(self):
        pass
    
    @abstractmethod
    def pattern_applies(self, event_log) -> bool:
        """
        checks whether the pattern applies 
        
        Arguments
        -----------
        event_log : EventLogStorage
            the EventLogStorage object of the log on which the conditions of the pattern should be checked
        
        Returns
        -----------
        bool
            True if the pattern applies, False if it does not apply
        """
        pass
        
    def check_dependencies(self):
        """
        checks whether the pattern already applies without further checking based on dependencies with other classes and logs it
        
        Returns
        -----------
        bool
            True if the pattern already applies without further checking, False if it cannot apply anymore, or otherwise None if no statement can be made
        """
        pattern_logger.info(f"\nChecking {self.name}:")
        if self.applies is None:
            return None
        elif self.applies:
            pattern_logger.info(f"\t>>>\t{self.name} was forced/implied by another pattern")
            return True
        elif not self.applies:
            pattern_logger.info(f"\t>>>\t{self.name} cannot apply because preconditions are violated")
            return False
    
