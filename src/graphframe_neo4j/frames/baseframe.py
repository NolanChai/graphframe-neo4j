"""
BaseFrame: Common functionality for all frame types.
"""

from typing import Any, Dict, List, Optional, Tuple
from ..graph import Graph

# Optional pandas integration
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    pd = None

# Optional networkx integration
try:
    import networkx as nx
    HAS_NETWORKX = True
except ImportError:
    HAS_NETWORKX = False
    nx = None

class BaseFrame:
    """
    Base class for all frame types (NodeFrame, EdgeFrame, PathFrame).
    """
    
    def __init__(self, graph: Graph, label: str):
        self._graph = graph
        self._label = label
        self._filters: List[Dict[str, Any]] = []
        self._selected_fields: List[str] = []
        self._limit: Optional[int] = None
        self._offset: Optional[int] = None
        self._order_by: List[Tuple[str, str]] = []  # (field, direction)
    
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} label={self._label}>"
    
    @property
    def graph(self) -> Graph:
        """Get the associated Graph instance."""
        return self._graph
    
    @property
    def label(self) -> str:
        """Get the label/type being queried."""
        return self._label

    def to_df(self) -> "pd.DataFrame":
        """
        Execute the query and return results as a pandas DataFrame.
        
        Requires pandas to be installed (install with `uv add pandas`).
        
        Returns:
            pd.DataFrame: Query results as a DataFrame
            
        Raises:
            ImportError: If pandas is not installed
        """
        if not HAS_PANDAS:
            raise ImportError(
                "pandas is required for to_df(). Install with: uv add pandas"
            )
        
        # Execute the query and get records
        records = self.to_records()
        
        # Convert to DataFrame
        return pd.DataFrame(records)
