"""
Data models for Elia OpenData API responses.
"""
from typing import Dict, List, Optional, Any
from datetime import datetime
import json

class BaseModel:
    """Base class for all Elia data models."""
    
    def __init__(self, data: Dict[str, Any]):
        """
        Initialize the model with raw API data.
        
        Args:
            data: Raw API response data
        """
        self._raw = data
        
    @property
    def raw(self) -> Dict[str, Any]:
        """Get the raw API response data."""
        return self._raw
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert the model to a dictionary."""
        return self._raw
        
    def to_json(self) -> str:
        """Convert the model to a JSON string."""
        return json.dumps(self.to_dict())
    
    def _ensure_dependencies(self, lib_name: str) -> Any:
        """
        Ensure required dependencies are installed.
        
        Args:
            lib_name: Name of the library to import
            
        Returns:
            Imported library module
        
        Raises:
            ImportError: If the library is not installed
        """
        try:
            return __import__(lib_name)
        except ImportError:
            raise ImportError(
                f"The '{lib_name}' package is required for this operation. "
                f"Please install it using: pip install {lib_name}"
            )

    def to_pandas(self):
        """Convert the model to a pandas DataFrame."""
        pd = self._ensure_dependencies("pandas")
        return pd.DataFrame(self.to_dict())
    
    def to_numpy(self):
        """Convert the model to a numpy array."""
        np = self._ensure_dependencies("numpy")
        return np.array(self.to_dict())
    
    def to_polars(self):
        """Convert the model to a polars DataFrame."""
        pl = self._ensure_dependencies("polars")
        return pl.DataFrame(self.to_dict())
    
    def to_arrow(self):
        """Convert the model to an Arrow table."""
        pa = self._ensure_dependencies("pyarrow")
        return pa.Table.from_pydict(self.to_dict())

class CatalogEntry(BaseModel):
    """
    Represents a dataset entry in the Elia OpenData catalog.
    Maps to /api/v2/catalog/datasets response items.
    """
    
    def __init__(self, data: Dict[str, Any]):
        super().__init__(data)
        self.id: str = data.get("dataset_id", "")
        # Try to get title from metas['default']['title'] if present, else fallback to top-level 'title'
        metas = data.get("metas", {})
        default_meta = metas.get("default", {}) if isinstance(metas, dict) else {}
        self.title: str = default_meta.get("title") or data.get("title", "")
        self.description: str = default_meta.get("description") or data.get("description", "")
        self.theme: str = default_meta.get("theme") or data.get("theme", "")
        self.modified: Optional[datetime] = None
        self.features: List[str] = data.get("features", [])
        self.name: str = data.get("name", "")
        if modified := default_meta.get("modified") or data.get("modified"):
            try:
                self.modified = datetime.fromisoformat(modified.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                pass

class DatasetMetadata(BaseModel):
    """
    Represents detailed metadata for a specific dataset.
    Maps to /api/v2/catalog/datasets/{dataset_id} response.
    """
    
    def __init__(self, data: Dict[str, Any]):
        super().__init__(data)
        self.id: str = data.get("dataset_id", "")
        self.title: str = data.get("title", "")
        self.description: str = data.get("description", "")
        self.theme: str = data.get("theme", "")
        self.modified: Optional[datetime] = None
        self.features: List[str] = data.get("features", [])
        self.fields: List[Dict] = data.get("fields", [])
        self.attachments: List[Dict] = data.get("attachments", [])
        
        if modified := data.get("modified"):
            try:
                self.modified = datetime.fromisoformat(modified.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                pass

class Records(BaseModel):
    """
    Represents records from a dataset.
    Maps to /api/v2/catalog/datasets/{dataset_id}/records response.
    """
    
    def __init__(self, data: Dict[str, Any]):
        super().__init__(data)
        self.total_count: int = data.get("total_count", 0)
        self.records: List[Dict] = data.get("records", [])
        self.links: List[Dict] = data.get("links", [])
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert only the records to a dictionary."""
        return {"records": self.records}
    
    @property
    def has_next(self) -> bool:
        """Check if there are more records available."""
        return any(link.get("rel") == "next" for link in self.links)