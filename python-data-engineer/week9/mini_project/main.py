"""
Main application entry point.
"""

import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from pathlib import Path

from src.core.config import config
from src.core.database import db_manager
from src.api.routes import api_router

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_app() -> FastAPI:
    """Create FastAPI application."""
    app = FastAPI(
        title="Analytics Platform",
        description="Real-time analytics platform with multiple data sources",
        version="1.0.0"
    )
    
    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Include API routes
    app.include_router(api_router, prefix="/api")
    
    @app.on_event("startup")
    async def startup():
        """Application startup handler."""
        try:
            # Initialize database
            db_manager.init_db()
            logger.info("Application started successfully")
        except Exception as e:
            logger.error(f"Application startup failed: {e}")
            raise
    
    @app.on_event("shutdown")
    async def shutdown():
        """Application shutdown handler."""
        try:
            # Cleanup resources
            db_manager.close()
            logger.info("Application shutdown successfully")
        except Exception as e:
            logger.error(f"Application shutdown failed: {e}")
    
    return app

def main():
    """Main execution."""
    try:
        # Create application
        app = create_app()
        
        # Run server
        uvicorn.run(
            app,
            host=config.get('api.host', '0.0.0.0'),
            port=config.get('api.port', 8000)
        )
        
    except Exception as e:
        logger.error(f"Application execution failed: {e}")
        raise

if __name__ == "__main__":
    main() 