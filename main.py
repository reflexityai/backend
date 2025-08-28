from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd
import pg8000
import os
from dotenv import load_dotenv
from datetime import datetime
import io 
from sqlalchemy import create_engine
import time
from fastapi import Request
import logging

# Load environment variables
load_dotenv()

app = FastAPI()

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

@app.post("/api/ingest-file")
async def ingest_file(file: UploadFile = File(...)):
    """
    Ingest CSV or XLSX file and store in database under raw schema
    """
        
    # Step 1: Validate file
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")
    file_extension = file.filename.lower().split('.')[-1]
    if file_extension not in ['csv', 'xlsx', 'xls']:
        raise HTTPException(
            status_code=400, 
            detail="Unsupported file type. Please upload CSV or Excel files only."
        )
    
    try:
        # Step 2: Read file content(bytes) and convert to BytesIO object
        file_content = await file.read()
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
        safe_filename = sanitize_string(file.filename)
        table_name = f"raw_{safe_filename}_{timestamp}"
        
        # Step 6: Clean column names
        df.columns = [sanitize_string(col, to_lowercase=True) for col in df.columns]
        
        # Step 7: Bulk database operations using SQLAlchemy
        engine = get_sqlalchemy_engine()
        
        try:
            # Step 7a: Bulk insert using pandas to_sql
            # Use pandas to_sql for bulk insertion (much faster)
            start_time = time.time()
            print("ingesting data.....")
            result = df.to_sql(
                name=table_name,
                con=engine,  # TODO: use pg800
                schema='raw',
                if_exists='replace',  # Replace if table exists
                index=False,  # Don't include DataFrame index
                method='multi',  # Use multi-row insert
                chunksize=1000  # Process in chunks of 1000
            )
            end_time = time.time()
            print(f"Time taken: {end_time - start_time} seconds")

            # verify the data
            if result == len(df):
                print("✅ Data ingested successfully")
            elif result != 0:
                print("❌ Data ingestion failed, but some rows were ingested")
            else:
                print("❌ Data ingestion failed")
            
            # Step 8: Return success response
            response_data = {
                "message": "File ingested successfully",
                "table_name": table_name,
                "rows_processed": len(df),
                "columns": list(df.columns),
                "file_name": file.filename,
                "verified_rows": result
            }
            return JSONResponse(
                status_code=200,
                content=response_data
            )
            
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database operation failed: {str(e)}")
        finally:
            engine.dispose()
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"File processing failed: {str(e)}")
    

@app.post("/api/upload-webhook")
async def upload_webhook(request: Request):
    """
    Handle webhook from Supabase storage
    """
    logging.info(request)
    print(request)
    return JSONResponse(status_code=200, content={"message": "Webhook received"})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

# Add this for Vercel
app.debug = True