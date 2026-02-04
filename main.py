"""
This file exists mainly to use the FastAPI CLI.
As it's named main.py it's automatically discovered.
"""

from artistic_intelligence_data.api import app

# public export, prevents pylint from removing the import
__all__ = ["app"]
