import boto3
import csv
import io
import logging

# Set up the logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # Log the event details to track the input event
    logger.info("Received event: %s", event)
    
    # Extract bucket and file details from the event
    try:
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
        destination_bucket = 'etl-cleaned-data'  # Replace with your destination bucket name
        
        logger.info("Source bucket: %s", source_bucket)
        logger.info("Object key: %s", object_key)
        logger.info("Destination bucket: %s", destination_bucket)
    except KeyError as e:
        logger.error("Error extracting event data: %s", e)
        raise ValueError(f"Missing expected field in event: {e}")

    s3 = boto3.client('s3')

    try:
        # Extract: Read the file from S3
        logger.info("Fetching file from S3...")
        response = s3.get_object(Bucket=source_bucket, Key=object_key)
        raw_data = response['Body'].read().decode('utf-8').splitlines()
        logger.info("File fetched successfully. Size: %d bytes", len(raw_data))
    except Exception as e:
        logger.error("Error fetching file from S3: %s", e)
        raise

    # Transform: Clean and process data
    cleaned_data = []
    try:
        logger.info("Cleaning data...")
        reader = csv.reader(raw_data)
        header = next(reader)  # Read the header
        cleaned_data.append(header)  # Keep the header
        logger.info("Header: %s", header)

        for row in reader:
            # Log the row before processing for visibility
            logger.debug("Processing row: %s", row)

            # Filter out rows with missing values (e.g., null Age)
            if row[2] != '':
                cleaned_data.append(row)
            else:
                logger.info("Skipping row due to missing data: %s", row)
        
        logger.info("Data cleaning complete. Processed %d rows.", len(cleaned_data) - 1)
    except Exception as e:
        logger.error("Error processing data: %s", e)
        raise

    # Load: Write the cleaned data back to S3
    try:
        logger.info("Writing cleaned data to S3...")
        output_buffer = io.StringIO()
        writer = csv.writer(output_buffer)
        writer.writerows(cleaned_data)
        
        s3.put_object(
            Bucket=destination_bucket,
            Key=f"cleaned_{object_key}",
            Body=output_buffer.getvalue()
        )
        
        logger.info("File saved to S3 as cleaned_%s", object_key)
    except Exception as e:
        logger.error("Error writing to S3: %s", e)
        raise

    return {
        'statusCode': 200,
        'body': f"File processed and saved to {destination_bucket}/cleaned_{object_key}"
    }