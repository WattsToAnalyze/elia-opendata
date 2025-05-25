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
        return pd.DataFrame([self.to_dict()])
    
    def to_numpy(self):
        """Convert the model to a numpy array."""
        np = self._ensure_dependencies("numpy")
        pd = self._ensure_dependencies("pandas")
        df = pd.DataFrame([self.to_dict()])
        return df.to_numpy()
    
    def to_polars(self):
        """Convert the model to a polars DataFrame."""
        pl = self._ensure_dependencies("polars")
        pd = self._ensure_dependencies("pandas")
        df = pd.DataFrame([self.to_dict()])
        return pl.from_pandas(df)

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
        
        # Extract dataset part
        dataset = data.get("dataset", {}) if "dataset" in data else data
        self.id = dataset.get("dataset_id", "")
        
        # Get metadata from default.metas
        metas = dataset.get("metas", {})
        default_meta = metas.get("default", {}) if isinstance(metas, dict) else {}
        
        # Set attributes from default_meta or fallback to top-level
        self.title = default_meta.get("title", dataset.get("title", ""))
        self.description = default_meta.get("description", dataset.get("description", ""))
        self.theme = default_meta.get("theme", dataset.get("theme", []))
        
        # Set other attributes
        self.features = dataset.get("features", [])
        self.fields = dataset.get("fields", [])
        self.theme: List[str] = default_meta.get("theme", [])
        self.modified: Optional[datetime] = None
        self.features: List[str] = dataset.get("features", [])
        self.fields: List[Dict] = dataset.get("fields", [])
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
        dataset = data.get("dataset", {})
        self.id: str = dataset.get("dataset_id", "")
        
        # Get metadata from dataset.metas.default structure
        metas = dataset.get("metas", {})
        default_meta = metas.get("default", {}) if isinstance(metas, dict) else {}
        
        self.title: str = default_meta.get("title", "")
        self.description: str = default_meta.get("description", "")
        self.theme: List[str] = default_meta.get("theme", [])
        self.modified: Optional[datetime] = None
        
        self.features: List[str] = dataset.get("features", [])
        self.fields: List[Dict] = dataset.get("fields", [])
        self.attachments: List[Dict] = dataset.get("attachments", [])
        
        if modified := default_meta.get("modified"):
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

    def _flatten_records(self) -> List[Dict[str, Any]]:
        """Flatten the nested records structure for data conversion."""
        flattened = []
        for record in self.records:
            if isinstance(record, dict) and "record" in record:
                record_data = record["record"]
                fields = record_data.get("fields", {})
                flat_record = {
                    "id": record_data.get("id", ""),
                    "timestamp": record_data.get("timestamp", ""),
                    "size": record_data.get("size", 0),
                    **fields
                }
                flattened.append(flat_record)
        return flattened
    
    def to_pandas(self):
        """Convert records to a pandas DataFrame with flattened structure."""
        pd = self._ensure_dependencies("pandas")
        flattened_records = self._flatten_records()
        return pd.DataFrame(flattened_records)
    
    def to_numpy(self):
        """Convert records to a numpy array with flattened structure."""
        np = self._ensure_dependencies("numpy")
        pd = self._ensure_dependencies("pandas")
        df = self.to_pandas()
        return df.to_numpy()
    
    def to_polars(self):
        """Convert records to a polars DataFrame with flattened structure."""
        pl = self._ensure_dependencies("polars")
        flattened_records = self._flatten_records()
        return pl.DataFrame(flattened_records)
    
    @property
    def has_next(self) -> bool:
        """Check if there are more records available."""
        return any(link.get("rel") == "next" for link in self.links)