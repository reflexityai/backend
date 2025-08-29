from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import JSONResponse
import pandas as pd
import pg8000
import os
from dotenv import load_dotenv
from datetime import datetime
import io 
from sqlalchemy import create_engine
import logging
import json
import asyncio
from supabase import create_client, Client
import re
import logfire

# Load environment variables
load_dotenv()

# Initialize FastAPI app
app = FastAPI()

logfire.configure()
logfire.instrument_fastapi(app)

# Initialize Supabase client
def get_supabase_client() -> Client:
    """Create and return Supabase client"""
    url: str = os.environ.get("SUPABASE_URL")
    key: str = os.environ.get("SERVICE_ROLE_KEY")
    if not url or not key:
        raise HTTPException(status_code=500, detail="Supabase configuration missing")
    logfire.info("Supabase client created")
    return create_client(url, key)

def sanitize_string(input_string: str, to_lowercase: bool = True) -> str:
    """
    Sanitize string for safe database naming (table names, column names, etc.)
    
    Args:
        input_string: The string to sanitize
        to_lowercase: Whether to convert to lowercase (default: True)
    
    Returns:
        Sanitized string with all special characters replaced with underscores
    """
    # Use regex to replace all non-alphanumeric characters with underscores
    # This includes: spaces, dots, hyphens, commas, parentheses, brackets, etc.
    sanitized = re.sub(r'[^a-zA-Z0-9]', '_', input_string)
    # Remove multiple consecutive underscores
    sanitized = re.sub(r'_+', '_', sanitized)
    # Remove leading and trailing underscores
    sanitized = sanitized.strip('_')
    # Convert to lowercase if requested
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
        logfire.info("Database connection established successfully")
        return connection
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

# Get SQLAlchemy engine for bulk operations
def get_sqlalchemy_engine():
    """Create and return SQLAlchemy engine for bulk operations"""
    try:
        connection_string = f"postgresql+pg8000://{os.getenv('user')}:{os.getenv('password')}@{os.getenv('host')}:{os.getenv('port')}/{os.getenv('dbname')}"
        engine = create_engine(connection_string)
        logfire.info("SQLAlchemy engine created successfully")
        return engine
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SQLAlchemy engine creation failed: {str(e)}")

# Create raw schema if it doesn't exist
def ensure_raw_schema():
    """Ensure the raw schema exists in the database"""
    connection = get_db_connection()
    cursor = connection.cursor()
    
    try:
        # Create raw schema if it doesn't exist
        cursor.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        connection.commit()
        logfire.info("Raw schema created/verified successfully")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error ensuring raw schema: {e}")
    finally:
        cursor.close()
        connection.close()
        logfire.info("Database connection closed")

@app.get("/")
async def root():
    response_data = {"message": "Hello from Reflexity Backend!"}
    logfire.info("Root endpoint accessed", response_data=response_data)
    return response_data

@app.post("/api/upload-webhook")
async def upload_webhook(request: Request):
    """
    Handle webhook from Supabase storage
    """
    with logfire.span("webhook_processing", webhook_endpoint="/api/upload-webhook"):
        try:
            # Get the raw body
            body = await request.body()
            body_str = body.decode()
            logfire.info("Webhook request received",request_body=body_str,content_length=len(body_str))
            
            # Parse the webhook payload
            try:
                webhook_data = json.loads(body_str)
                logfire.info("Webhook payload parsed",webhook_type=webhook_data.get("type"),webhook_table=webhook_data.get("table"),bucket_id=webhook_data.get("record", {}).get("bucket_id"))
            except json.JSONDecodeError as e:
                raise HTTPException(status_code=400, detail=f"Invalid JSON payload: {e}")
            
            # Check if this is an INSERT event for the "raw" bucket
            if (webhook_data.get("type") == "INSERT" and 
                webhook_data.get("table") == "objects" and
                webhook_data.get("record", {}).get("bucket_id") == "raw"):
                
                record = webhook_data.get("record", {})
                file_name = record.get("name")
                file_path = "/".join(record.get("path_tokens", []))
                
                logfire.info("Processing raw bucket INSERT", file_name=file_name,file_path=file_path,record_id=record.get("id"),full_record=record)
                
                if not file_name or not file_path:
                    raise HTTPException(status_code=400, detail="Missing file information")
                
                # Process the file in background
                try:
                    # Start background task
                    logfire.info("Starting background task", file_name=file_name,file_path=file_path)
                    asyncio.create_task(process_uploaded_file(file_name, file_path))
                    
                    response_data = {
                        "message": "File processing started",
                        "file_name": file_name,
                        "file_path": file_path,
                        "status": "processing"
                    }
                    
                    # Return immediate response
                    logfire.info("Webhook response sent", response_data=response_data,status_code=202)
                    return JSONResponse(status_code=202, content=response_data)
                    
                except Exception as e:
                    raise HTTPException(status_code=500, detail=f"Error starting file processing: {str(e)}")
            

            # if not a raw bucket INSERT, return 200
            response_data = {"message": "Webhook received (not a raw bucket INSERT)"}
            return JSONResponse(status_code=200, content=response_data)
            
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error: {e}")

async def process_uploaded_file(file_name: str, file_path: str) -> dict:
    """
    Process uploaded file from Supabase storage similar to ingest_file function
    """
    with logfire.span("file_processing", file_name=file_name, file_path=file_path):
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
            logfire.info("File extension validated", file_extension=file_extension)
            
            # Step 2: Fetch file from Supabase storage
            supabase = get_supabase_client()
            
            # Download the file from storage
            file_content = supabase.storage.from_("raw").download(file_path)
            file_size = len(file_content)
            logfire.info("File downloaded", file_size_bytes=file_size,file_size_mb=round(file_size / 1024 / 1024, 2))
            
            # Step 3: Parse file
            file_buffer = io.BytesIO(file_content)
            if file_extension == 'csv':
                df = pd.read_csv(file_buffer)
            elif file_extension == 'xlsx':
                df = pd.read_excel(file_buffer)
            else: 
                raise HTTPException(
                    status_code=400, 
                    detail="Unsupported file type. Please upload CSV or Excel files only."
                )
            logfire.info("File parsed successfully", rows=len(df),columns=len(df.columns),column_names=list(df.columns))
            
            # Step 4: Ensure schema exists
            ensure_raw_schema()
            
            # Step 5: Generate table name
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename_with_ext = file_name.split('/')[-1]
            safe_filename = sanitize_string(filename_with_ext)        
            table_name = f"raw_{safe_filename}_{timestamp}"
            logfire.info("Table name generated", table_name=table_name,timestamp=timestamp,safe_filename=safe_filename)
            
            # Step 6: Clean column names
            original_columns = list(df.columns)
            df.columns = [sanitize_string(col, to_lowercase=True) for col in df.columns]
            cleaned_columns = list(df.columns)
            logfire.info("Column names cleaned", original_columns=original_columns,cleaned_columns=cleaned_columns)
            
            # Step 7: Bulk database operations using SQLAlchemy
            engine = get_sqlalchemy_engine()
            
            try:
                # Step 7a: Bulk insert using pandas to_sql
                with logfire.span("bulk_insert", table_name=table_name,rows_to_insert=len(df),chunk_size=1000):
                    result = df.to_sql(
                        name=table_name,
                        con=engine,
                        schema='raw',
                        if_exists='replace',
                        index=False,
                        method='multi',
                        chunksize=1000
                    )


                # Step 7b: Verify the data
                if result == len(df):
                    logfire.info("File processed successfully", table_name=table_name, rows_processed=result)
                elif result != 0:
                    logfire.warning("File partially processed", table_name=table_name, rows_processed=result,expected_rows=len(df))
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
                
                logfire.info("File processing completed successfully",response_data=response_data)
                return response_data
                
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Database operation failed: {str(e)}")
            finally:
                engine.dispose()
                logfire.info("Database engine disposed")
                
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"File processing failed: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)