"""
API data schemas.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum

class UserRole(str, Enum):
    """User role enumeration."""
    ADMIN = "admin"
    EDITOR = "editor"
    VIEWER = "viewer"

class UserBase(BaseModel):
    """Base user schema."""
    username: str
    email: str
    role: UserRole = UserRole.VIEWER

class UserCreate(UserBase):
    """User creation schema."""
    password: str

class User(UserBase):
    """User schema."""
    id: int
    created_at: datetime
    
    class Config:
        orm_mode = True

class Token(BaseModel):
    """Authentication token schema."""
    access_token: str
    token_type: str = "bearer"

class DataSourceConfig(BaseModel):
    """Data source configuration schema."""
    type: str
    name: str
    config: Dict[str, Any]

class ChartConfig(BaseModel):
    """Chart configuration schema."""
    type: str
    title: str
    x_axis: Dict[str, Any]
    y_axis: Dict[str, Any]
    data_source: str
    query: Dict[str, Any]
    theme: Optional[str] = "light"

class DashboardCreate(BaseModel):
    """Dashboard creation schema."""
    name: str
    layout: Dict[str, Any]
    charts: List[ChartConfig]

class DashboardUpdate(BaseModel):
    """Dashboard update schema."""
    name: Optional[str]
    layout: Optional[Dict[str, Any]]
    charts: Optional[List[ChartConfig]]

class AlertConfig(BaseModel):
    """Alert configuration schema."""
    name: str
    condition: Dict[str, Any]
    visualization_id: int
    notification: Dict[str, Any]

class ThemeConfig(BaseModel):
    """Theme configuration schema."""
    name: str
    background_color: str
    paper_color: str
    font_color: str
    grid_color: str
    colorway: List[str] 