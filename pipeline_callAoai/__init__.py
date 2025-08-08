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
from bs4 import BeautifulSoup
from datetime import datetime, timezone
 
# Define batch size (adjust based on LLM token limits)
BATCH_SIZE = 10
 
def process_blob(blob):
    """Extract relevant Excel content for LLM validation."""
    blob_name = blob.get("name")
    container_name = blob.get("container", "silver")
    # result = {"blob_name": blob_name, "excel_rows": [], "error": None}
    # result = {"blob_name": blob_name, "excel_rows": [], "taxonomy_data": None, "error": None}
    result = {"blob_name": blob_name, "excel_rows": [], "taxonomy_data": [],"unique_periods": [],"statement_of_compliance_text": None, "error": None}
 
 
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
            # df = pd.read_excel(xls, sheet_name='Filing Details')
 
            # # Extract required columns
            # df = df[['Line Item Description', 'Concept Label', 'Comment Text']].dropna(how='all')
            df_filing_details = pd.read_excel(xls, sheet_name='Filing Details')
 
            # Extract relevant columns for LLM validation
            df = df_filing_details[['Line Item Description', 'Concept Label', 'Comment Text','Dimensions']].dropna(how='all')
 
            # Extract unique 'Period' values
            if 'Period' in df_filing_details.columns:
                unique_periods = df_filing_details['Period'].dropna().unique().tolist()
                logging.info(f"[{blob_name}] Extracted Periods from Excel: {unique_periods}")
 
            else:
                unique_periods = []
                logging.info(f"[{blob_name}] Extracted Periods from Excel: {unique_periods}")
 
            result["excel_rows"] = df.to_dict(orient='records')
            result["unique_periods"] = unique_periods
 
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
       
        elif ext == '.html':
            try:
                soup = BeautifulSoup(blob_bytes.decode('utf-8', errors='ignore'), 'html.parser')
 
                start_tag = None
                for p in soup.find_all("p"):
                    if "STATEMENT OF COMPLIANCE" in p.get_text(strip=True).upper():
                        start_tag = p
                        break
 
                content = []
                if start_tag:
                    current = start_tag
                    while current:
                        text = current.get_text(strip=True)
                        if text.startswith("2.") and "ACCOUNTING POLICIES" in text.upper():
                            break
                        if text:
                            content.append(text)
                        current = current.find_next_sibling("p")
 
                result["statement_of_compliance_text"] = "\n".join(content)
 
            except Exception as e:
                result["error"] = f"Error extracting HTML content from {blob_name}: {str(e)}"
 
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
        # Use the user prompt from backend instead of constructing it manually
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
            logging.info(f'ROW BY ROW VALIDATION --> : {parsed_response}')
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
 
def validate_periods_with_llm(unique_periods, input_dates):
    prompts = load_prompts()
    system_prompt = prompts.get("system_prompt_period_validation", "")
    user_prompt_template = prompts.get("user_prompt_period_validation", "")
 
    try:
        user_prompt = user_prompt_template.format(
            periods=json.dumps(unique_periods, indent=2),
            input_dates=json.dumps(input_dates, indent=2)
        )
 
        response = run_prompt(system_prompt, user_prompt).strip()
        logging.info(f'PERIOD VALIDATION LLM RESPONSE: {response}')
 
        # Clean LLM formatting
        if response.startswith("```json"):
            response = response.strip("`").replace("json", "", 1).strip()
        elif response.startswith("```"):
            response = response.strip("`").strip()
 
        if not response.startswith("[") and not response.startswith("{"):
            logging.error("LLM period validation response is not valid JSON format")
            return [{"error": "Invalid period validation response format from LLM"}]
 
        return json.loads(response)
 
    except Exception as e:
        logging.error(f"Period validation LLM processing error: {str(e)}")
        return [{"error": f"Period validation failed: {str(e)}"}]
 
 
def _main_logic(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
   
    req_body = req.get_json()
    selected_blobs = req_body.get("blobs", None)
    input_dates = req_body.get("selectedDates", [])
 
    # input_dates = req_body.get("input_dates", [])  # Expecting UI to send dates here
    # input_dates = {
    # "end_date_current": "2023-12-31",
    # "duration_current": {
    #     "start": "2023-01-01",
    #     "end": "2023-12-31"
    # },
    # "end_date_prior": "2022-12-31",
    # "duration_prior": {
    #     "start": "2022-01-01",
    #     "end": "2022-12-31"
    # },
    # "opening_date_prior": "2021-12-31"
    # }
 
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
    #     for future in as_completed(futures):
    #         res = future.result()
    #         if res["error"]:
    #             errors.append(res["error"])
    #         if res["excel_rows"]:
    #             validated_data.extend(validate_with_llm(res["excel_rows"]))
    #         if res["taxonomy_data"]:
    #             taxonomy_data_to_validate.extend(res["taxonomy_data"])
 
    # if taxonomy_data_to_validate:
    #     validated_data.append(validate_taxonomy_with_llm(taxonomy_data_to_validate))
    # else:
    #     logging.warning("No taxonomy data found across all blobs.")
        # First: collect all data from blobs
        blob_results = []
        all_periods = []
        for future in as_completed(futures):
            res = future.result()
            blob_results.append(res)
            if res["error"]:
                errors.append(res["error"])
            if res["taxonomy_data"]:
                taxonomy_data_to_validate.extend(res["taxonomy_data"])
                # Inject HTML 'Statement of Compliance' into taxonomy validation
            if res.get("statement_of_compliance_text"):
                taxonomy_data_to_validate.append({
                    "source": "html_statement_of_compliance",
                    "content": res["statement_of_compliance_text"]
                })
            all_periods.extend(res.get("unique_periods", []))
 
 
        # first: validate taxonomy first
        if taxonomy_data_to_validate:
            taxonomy_result = validate_taxonomy_with_llm(taxonomy_data_to_validate)
            validated_data.append({"taxonomy_validation": taxonomy_result})
        else:
            logging.warning("No taxonomy data found across all blobs.")
        # second : validate the dates
        if input_dates:
            period_validation_result = validate_periods_with_llm(all_periods, input_dates)
            validated_data.append({"period_validation": period_validation_result})
        else:
            logging.warning("No input dates provided for period validation.")
 
        # Third: now validate Excel rows after taxonomy is validated
        for res in blob_results:
            if res["excel_rows"]:
                validated_data.extend(validate_with_llm(res["excel_rows"]))
 
 
    # Validate JSON
    try:
        json.dumps(validated_data)
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON from LLM: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON format from LLM"}),
            status_code=500,
            mimetype="application/json"
        )
 
    # Save to blob
    # output_name = "validated-output.json"
 
    # timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    # output_name = f"validated-output-{timestamp}.json"
    # write_to_blob("gold", output_name, json.dumps(validated_data, indent=2).encode('utf-8'))

    # Use first blob name for output naming
    first_blob_name = selected_blobs[0]["name"]
    base_filename = os.path.splitext(os.path.basename(first_blob_name))[0]
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    output_name = f"{base_filename}-validated-output-{timestamp}.json"
 
    write_to_blob("gold", output_name, json.dumps(validated_data, indent=2).encode('utf-8'))
 
    return func.HttpResponse(
        json.dumps({
            "processedFiles": [b["name"] for b in selected_blobs],
            "errors": errors,
            "outputFile": output_name,
            # "validated_data": validated_data,
            "status": "completed" if not errors else "completed_with_errors"
        }),
        status_code=200,
        mimetype="application/json"
    )
 
def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        return _main_logic(req)
    except Exception as e:
        logging.error(f"Fatal error in main: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
)