from concurrent.futures import ThreadPoolExecutor, as_completed
import azure.functions as func
import logging
import json
import io
import os
import pandas as pd
from utils.prompts import load_prompts
from utils.blob_functions import get_blob_content, write_to_blob
from utils.azure_openai import run_prompt

# Define batch size (adjust based on LLM token limits)
BATCH_SIZE = 10

def process_blob(blob):
    """Extract relevant Excel content for LLM validation."""
    blob_name = blob.get("name")
    container_name = blob.get("container", "silver")
    # result = {"blob_name": blob_name, "excel_rows": [], "error": None}
    # result = {"blob_name": blob_name, "excel_rows": [], "taxonomy_data": None, "error": None}
    result = {"blob_name": blob_name, "excel_rows": [], "taxonomy_data": [], "error": None} 


    if not blob_name:
        result["error"] = "Blob missing 'name'"
        return result

    if container_name == "bronze":
        result["error"] = f"Blobs from 'bronze' container not allowed: {blob_name}"
        return result

    try:
        blob_bytes = get_blob_content(container_name, blob_name)
        ext = os.path.splitext(blob_name)[1].lower()

        if ext in ['.xlsx', '.xls']:
            xls = pd.ExcelFile(io.BytesIO(blob_bytes))
            df = pd.read_excel(xls, sheet_name='Filing Details')

            # Extract required columns
            df = df[['Line Item Description', 'Concept Label', 'Comment Text']].dropna(how='all')

            # Convert each row into a dictionary
            result["excel_rows"] = df.to_dict(orient='records')

            # taxonomy_df = pd.read_excel(xls, sheet_name='Filing Information')
            # result["taxonomy_data"] = taxonomy_df.to_dict(orient='records')
            if 'Filing Information' in xls.sheet_names:
                taxonomy_df = pd.read_excel(xls, sheet_name='Filing Information')
                if not taxonomy_df.empty:
                    result["taxonomy_data"] = taxonomy_df.to_dict(orient='records')
                else:
                    logging.warning(f"'Filing Information' sheet is empty in blob {blob_name}")
            else:
                logging.warning(f"'Filing Information' sheet missing in blob {blob_name}")


    except Exception as e:
        result["error"] = f"Error processing blob {blob_name}: {str(e)}"

    return result

def batch_rows(rows, batch_size):
    """Split rows into smaller batches."""
    for i in range(0, len(rows), batch_size):
        yield rows[i:i + batch_size]

def validate_with_llm(rows):
    """Send batches of rows to LLM for validation."""
    validated_rows = []
    prompts = load_prompts()  
    system_prompt = prompts["system_prompt"]
    user_prompt_template = prompts["user_prompt"]  

    for batch in batch_rows(rows, BATCH_SIZE):
        # ✅ Use the user prompt from backend instead of constructing it manually
        user_prompt = user_prompt_template.format(data=json.dumps(batch, indent=2))

        response = run_prompt(system_prompt, user_prompt)
        try:
            response = response.strip()

            # Handle Markdown formatting from LLM like ```json ... ```
            if response.startswith("```json"):
                response = response.strip("`").replace("json", "", 1).strip()
            elif response.startswith("```"):
                response = response.strip("`").strip()

            # Ensure it's still a string before proceeding
            if not isinstance(response, str):
                logging.error("LLM response is not a valid string.")
                validated_rows.append({"error": "Invalid LLM response type"})
                continue

            if not response.startswith("[") and not response.startswith("{"):
                logging.error("LLM response is not valid JSON format")
                validated_rows.append({"error": "Invalid JSON format from LLM"})
                continue

            parsed_response = json.loads(response)

            # Optional: skip if it's [{}] or [{}] * n
            if isinstance(parsed_response, list) and all(isinstance(item, dict) and not item for item in parsed_response):
                logging.warning("Skipping empty [{}] response from LLM")
                continue
            logging.info(f'HARHAR MAHADEV : {parsed_response}')
            validated_rows.extend(parsed_response)

        except json.JSONDecodeError as e:
            logging.error(f"JSON parsing error: {str(e)}")
            validated_rows.append({"error": f"Invalid JSON format from LLM: {str(e)}"})

    return validated_rows

def validate_taxonomy_with_llm(taxonomy_data):
    prompts = load_prompts()
    system_prompt = prompts.get("system_prompt_taxonomy", "")  # Use separate system prompt
    taxonomy_prompt = prompts["taxonomy"]

    try:
        user_prompt = taxonomy_prompt.format(data=json.dumps(taxonomy_data, indent=2))
        response = run_prompt(system_prompt, user_prompt).strip()
        logging.info(f'TAXANOMY:{response}')

        # Clean LLM formatting
        if response.startswith("```json"):
            response = response.strip("`").replace("json", "", 1).strip()
        elif response.startswith("```"):
            response = response.strip("`").strip()

        if not response.startswith("[") and not response.startswith("{"):
            logging.error("LLM taxonomy response is not valid JSON format")
            return [{"error": "Invalid taxonomy response format from LLM"}]

        return json.loads(response)

    except Exception as e:
        logging.error(f"Taxonomy LLM processing error: {str(e)}")
        return [{"error": f"Taxonomy validation failed: {str(e)}"}]

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    try:
        req_body = req.get_json()
        selected_blobs = req_body.get("blobs", None)

        if not selected_blobs:
            return func.HttpResponse(
                json.dumps({"error": "No blobs provided."}),
                status_code=400,
                mimetype="application/json"
            )

        errors = []
        validated_data = []
        taxonomy_data_to_validate = []

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(process_blob, blob) for blob in selected_blobs]
            for future in as_completed(futures):
                res = future.result()
                if res["error"]:
                    errors.append(res["error"])
                if res["excel_rows"]:
                    validated_data.extend(validate_with_llm(res["excel_rows"]))  # keep per blob Excel rows validation
                
                # if res["taxonomy_data"] and taxonomy_data_to_validate is None:
                #     taxonomy_data_to_validate = res["taxonomy_data"]

                if res["taxonomy_data"]:
                    taxonomy_data_to_validate.extend(res["taxonomy_data"])

        if taxonomy_data_to_validate:
            validated_data.append(validate_taxonomy_with_llm(taxonomy_data_to_validate))
        else:
            logging.warning("No taxonomy data found across all blobs.")
        # Now validate taxonomy only once outside the loop
        # if taxonomy_data_to_validate:
        #     validated_data.extend(validate_taxonomy_with_llm(taxonomy_data_to_validate))



        # ✅ Validate JSON before sending to UI
        try:
            parsed_response = json.loads(json.dumps(validated_data))  # Validate JSON
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON format received from LLM: {str(e)}")
            return func.HttpResponse(
                json.dumps({"error": "Invalid JSON format from LLM"}),
                status_code=500,
                mimetype="application/json"
            )

        # Save validated results to blob
        output_name = "validated-output.json"
        write_to_blob("gold", output_name, json.dumps(validated_data, indent=2).encode('utf-8'))

        return func.HttpResponse(
            json.dumps({
                "processedFiles": [b["name"] for b in selected_blobs],
                "errors": errors,
                "validated_data": validated_data,
                "status": "completed" if not errors else "completed_with_errors"
            }),
            status_code=200 if not errors else 500,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Unhandled error: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )