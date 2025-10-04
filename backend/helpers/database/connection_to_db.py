import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, event, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from typing import Generator
from contextlib import contextmanager
import sys
sys.path.insert(1, "../")
from helpers.logger import get_logger

logger = get_logger()
load_dotenv()

# PostgreSQL connection parameters from environment
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "dashboard_mvp")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Debug mode
DEBUG = os.getenv("DEBUG", "False").lower() == "true"


def get_db_url() -> str:
    """
    Construct PostgreSQL connection URL from environment variables
    
    Returns:
        str: Database connection URL
    """
    if not DB_PASSWORD:
        raise ValueError("DB_PASSWORD environment variable is not set")
    
    return f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def connect_to_postgresql():
    """
    Creates and returns a SQLAlchemy engine for PostgreSQL
    
    Returns:
        sqlalchemy.engine.Engine: Database engine instance
    """
    try:
        connection_string = get_db_url()
        
        # Create engine with connection pooling
        engine = create_engine(
            connection_string,
            poolclass=QueuePool,
            pool_size=10,              # Number of connections to maintain
            max_overflow=20,           # Max connections beyond pool_size
            pool_pre_ping=True,        # Verify connection before using
            pool_recycle=3600,         # Recycle connections after 1 hour
            echo=DEBUG,                # Log SQL queries in debug mode
        )
        
        logger.debug("PostgreSQL engine created successfully!")
        return engine
        
    except Exception as e:
        logger.error(f"Failed to create PostgreSQL engine: {e}", exc_info=True)
        return None


# Create engine
engine = connect_to_postgresql()

if engine is None:
    raise Exception("Failed to initialize database engine")

# Session factory
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

# Base class for ORM models
Base = declarative_base()


# Event listeners
@event.listens_for(engine, "connect")
def receive_connect(dbapi_conn, connection_record):
    """Log successful database connections"""
    logger.debug("PostgreSQL connection established")


@event.listens_for(engine, "close")
def receive_close(dbapi_conn, connection_record):
    """Log database disconnections"""
    logger.debug("PostgreSQL connection closed")


def get_db() -> Generator[Session, None, None]:
    """
    FastAPI dependency for database sessions
    Provides a database session and ensures it's closed after use
    
    Usage:
        @app.get("/files")
        def get_files(db: Session = Depends(get_db)):
            files = db.query(UploadedFile).all()
            return files
    
    Yields:
        Session: SQLAlchemy database session
    """
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        logger.error(f"Database session error: {e}", exc_info=True)
        db.rollback()
        raise
    finally:
        db.close()


@contextmanager
def get_db_context():
    """
    Context manager for database sessions
    Use this outside of FastAPI (e.g., in background tasks)
    
    Usage:
        with get_db_context() as db:
            files = db.query(UploadedFile).all()
    
    Yields:
        Session: SQLAlchemy database session
    """
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception as e:
        logger.error(f"Database context error: {e}", exc_info=True)
        db.rollback()
        raise
    finally:
        db.close()


def init_db():
    """
    Initialize database - create all tables defined in models
    Should be called on application startup
    """
    try:
        # Import all models here to ensure they're registered
        from helpers.database.models import (
            UploadedFile,
            RawData,
            CleanedData,
            ChartMapping,
            DashboardConfig,
            ChatHistory,
            DataEmbedding
        )
        
        Base.metadata.create_all(bind=engine)
        logger.info("✅ Database tables created successfully")
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize database: {e}", exc_info=True)
        raise


def check_db_connection() -> bool:
    """
    Check if database connection is working
    
    Returns:
        bool: True if connection is successful, False otherwise
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✅ Database connection test successful")
        return True
    except Exception as e:
        logger.error(f"❌ Database connection test failed: {e}", exc_info=True)
        return False


def close_db_connection():
    """
    Close all database connections
    Should be called on application shutdown
    """
    try:
        engine.dispose()
        logger.info("✅ Database connections closed")
    except Exception as e:
        logger.error(f"❌ Failed to close database connections: {e}", exc_info=True)
