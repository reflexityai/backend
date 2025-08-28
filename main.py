from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import JSONResponse
import pandas as pd
import pg8000
import os
from dotenv import load_dotenv
from datetime import datetime
import io 
from sqlalchemy import create_engine
import time
import logging
import json
import asyncio
from supabase import create_client, Client

# Load environment variables
load_dotenv()

# Initialize FastAPI app
app = FastAPI()

# Initialize Supabase client
def get_supabase_client() -> Client:
    """Create and return Supabase client"""
    url: str = os.environ.get("SUPABASE_URL")
    key: str = os.environ.get("SERVICE_ROLE_KEY")
    if not url or not key:
        raise HTTPException(status_code=500, detail="Supabase configuration missing")
    return create_client(url, key)

# Utility functions (TODO: regex)
def sanitize_string(input_string: str, to_lowercase: bool = False) -> str:
    """
    Sanitize string for safe database naming (table names, column names, etc.)
    
    Args:
        input_string: The string to sanitize
        to_lowercase: Whether to convert to lowercase (default: False)
    
    Returns:
        Sanitized string with dots, hyphens, and spaces replaced with underscores
    """
    sanitized = input_string.replace('.', '_').replace('-', '_').replace(' ', '_')
    result = sanitized.lower() if to_lowercase else sanitized
    return result

# Database connection function
def get_db_connection():
    """Create and return a database connection"""
    try:
        connection = pg8000.Connection(
            user=os.getenv("user"),
            password=os.getenv("password"),
            host=os.getenv("host"),
            port=os.getenv("port"),
            database=os.getenv("dbname")
        )
        return connection
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

# Get SQLAlchemy engine for bulk operations
def get_sqlalchemy_engine():
    """Create and return SQLAlchemy engine for bulk operations"""
    print("🔌 Creating SQLAlchemy engine...")
    try:
        connection_string = f"postgresql+pg8000://{os.getenv('user')}:{os.getenv('password')}@{os.getenv('host')}:{os.getenv('port')}/{os.getenv('dbname')}"
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SQLAlchemy engine creation failed: {str(e)}")

# Create raw schema if it doesn't exist
def ensure_raw_schema():
    """Ensure the raw schema exists in the database"""
    print("📁 Ensuring raw schema exists...")
    connection = get_db_connection()
    cursor = connection.cursor()
    
    try:
        # Create raw schema if it doesn't exist
        cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        connection.commit()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error ensuring raw schema: {e}")
    finally:
        cursor.close()
        connection.close()

@app.get("/")
async def root():
    return {"message": "Hello from Reflexity Backend!"}

@app.post("/api/upload-webhook")
async def upload_webhook(request: Request):
    """
    Handle webhook from Supabase storage
    """
    try:
        # Get the raw body
        body = await request.body()
        body_str = body.decode()
        
        # Parse the webhook payload
        try:
            webhook_data = json.loads(body_str)
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail=f"Invalid JSON payload: {e}")
        
        # Check if this is an INSERT event for the "raw" bucket
        if (webhook_data.get("type") == "INSERT" and 
            webhook_data.get("table") == "objects" and
            webhook_data.get("record", {}).get("bucket_id") == "raw"):
            
            record = webhook_data.get("record", {})
            file_name = record.get("name")
            file_path = "/".join(record.get("path_tokens", []))
            
            if not file_name or not file_path:
                raise HTTPException(status_code=400, detail="Missing file information")
            
            # Process the file in background
            try:
                # Start background task
                asyncio.create_task(process_uploaded_file(file_name, file_path))
                
                # Return immediate response
                return JSONResponse(
                    status_code=202, 
                    content={
                        "message": "File processing started",
                        "file_name": file_name,
                        "file_path": file_path,
                        "status": "processing"
                    }
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error starting file processing: {str(e)}")
        
        return JSONResponse(status_code=200, content={"message": "Webhook received (not a raw bucket INSERT)"})
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {e}")

async def process_uploaded_file(file_name: str, file_path: str) -> dict:
    """
    Process uploaded file from Supabase storage similar to ingest_file function
    """
    try:
        # Step 1: Validate file
        if not file_name:
            raise HTTPException(status_code=400, detail="No file name provided")
        
        file_extension = file_name.lower().split('.')[-1]
        if file_extension not in ['csv', 'xlsx', 'xls']:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported file type: {file_extension}. Please upload CSV or Excel files only."
            )
        
        # Step 2: Fetch file from Supabase storage
        supabase = get_supabase_client()
        
        # Download the file from storage
        file_content = supabase.storage.from_("raw").download(file_path)
        
        # Convert to BytesIO object
        file_buffer = io.BytesIO(file_content)
        
        # Step 3: Parse file
        if file_extension == 'csv':
            df = pd.read_csv(file_buffer)
        elif file_extension == 'xlsx':
            df = pd.read_excel(file_buffer)
        else: 
            raise HTTPException(
                status_code=400, 
                detail="Unsupported file type. Please upload CSV or Excel files only."
            )
        
        # Step 4: Ensure schema exists
        ensure_raw_schema()
        
        # Step 5: Generate table name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename_with_ext = file_name.split('/')[-1]
        safe_filename = sanitize_string(filename_with_ext)        
        table_name = f"raw_{safe_filename}_{timestamp}"
        
        # Step 6: Clean column names
        df.columns = [sanitize_string(col, to_lowercase=True) for col in df.columns]
        
        # Step 7: Bulk database operations using SQLAlchemy
        engine = get_sqlalchemy_engine()
        
        try:
            # Step 7a: Bulk insert using pandas to_sql
            # Use pandas to_sql for bulk insertion (much faster)
            logging.info(f"Ingesting data into table: {table_name}")
            result = df.to_sql(
                name=table_name,
                con=engine,  # TODO: use pg800
                schema='raw',
                if_exists='replace',  # Replace if table exists
                index=False,  # Don't include DataFrame index
                method='multi',  # Use multi-row insert
                chunksize=1000  # Process in chunks of 1000
            )

            # Step 7b: Verify the data
            if result == len(df):
                logging.info(f"File {file_name} processed successfully. Table: {table_name}, Rows: {result}")
            elif result != 0:
                logging.warning(f"File {file_name} partially processed. Table: {table_name}, Rows: {result}")
            else:
                raise HTTPException(status_code=500, detail=f"File {file_name} processing failed. Table: {table_name}")
            
            # Step 8: Return success response
            response_data = {
                "message": "File ingested successfully",
                "table_name": table_name,
                "rows_processed": len(df),
                "columns": list(df.columns),
                "file_name": file_name,
                "verified_rows": result,
                "source": "webhook"
            }
            return response_data
            
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database operation failed: {str(e)}")
        finally:
            engine.dispose()
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"File processing failed: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)