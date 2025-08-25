from concurrent.futures import ThreadPoolExecutor, as_completed
import azure.functions as func
import logging
import json
import io
import os
import pandas as pd
import re
from utils.prompts import load_prompts
from utils.blob_functions import get_blob_content, write_to_blob, list_blobs
from utils.azure_openai import run_prompt
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import Levenshtein
from typing import Tuple
from html2image import Html2Image
import tempfile
import base64


# Define batch size (adjust based on LLM token limits)
BATCH_SIZE = 10

def convert_html_to_images(html_bytes, html_blob_name):
    """
    Converts full HTML to multiple page images.
    Returns { page_number: image_bytes }.
    """
    try:
        html_content = html_bytes.decode('utf-8', errors='ignore')

        hti = Html2Image()
        image_map = {}

        with tempfile.TemporaryDirectory() as tmpdirname:
            hti.output_path = tmpdirname

            base_name = os.path.splitext(os.path.basename(html_blob_name))[0]
            timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
            image_prefix = f"{base_name}_{timestamp}_page"

            hti.screenshot(
                html_str=html_content,
                save_as=f"{image_prefix}.png",
                size=(1200, 1600),
                browser_width=1200
            )

            files = sorted(
                f for f in os.listdir(tmpdirname)
                if f.startswith(image_prefix) and f.endswith(".png")
            )

            for i, filename in enumerate(files):
                page_number = i + 1
                local_path = os.path.join(tmpdirname, filename)

                with open(local_path, "rb") as f:
                    image_bytes = f.read()

                image_map[page_number] = image_bytes

        return image_map

    except Exception as e:
        logging.error(f"❌ Error in convert_html_to_images: {str(e)}")
        return {}
 
def process_blob(blob):
    """Extract relevant Excel content for LLM validation."""
    blob_name = blob.get("name")
    container_name = blob.get("container", "silver")
    # result = {"blob_name": blob_name, "excel_rows": [], "error": None}
    # result = {"blob_name": blob_name, "excel_rows": [], "taxonomy_data": None, "error": None}
    result = {"blob_name": blob_name, "excel_rows": [], "taxonomy_data": [],"unique_periods": [],"statement_of_compliance_text": None, "error": None,"html_page_images":[],"html_file_hash":None}
 
 
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
            df = df_filing_details[['Line Item Description', 'Concept Label', 'Comment Text','Dimensions','Tag Value','Page Number']].dropna(how='all')
            # Clean Page Number column if present
            if 'Page Number' in df.columns:
                def clean_page_number(val):
                    import ast
                    try:
                        if isinstance(val, list):
                            return val[0] if val else None
                        if isinstance(val, str):
                            val = val.strip()
                            if val.startswith("[") and val.endswith("]"):
                                parsed = ast.literal_eval(val)
                                if isinstance(parsed, list) and parsed:
                                    return int(parsed[0])
                            elif val.isdigit():
                                return int(val)
                        if isinstance(val, float):
                            return int(val)
                        if isinstance(val, int):
                            return val
                        return None
                    except Exception:
                        return None

                df['Page Number'] = df['Page Number'].apply(clean_page_number)
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

        # 📸 Convert HTML to images
                logging.warning(f"🚀 Converting HTML to images for blob: {blob_name}")
                page_images = convert_html_to_images(blob_bytes, blob_name)
                result["html_page_images"] = page_images

            except Exception as e:
                result["error"] = f"Error extracting HTML content from {blob_name}: {str(e)}"
 
    except Exception as e:
        result["error"] = f"Error processing blob {blob_name}: {str(e)}"
 
    return result
 
def batch_rows(rows, batch_size):
    """Split rows into smaller batches."""
    for i in range(0, len(rows), batch_size):
        yield rows[i:i + batch_size]
 
def validate_with_llm(rows, html_images=None):
    """Send batches of rows to LLM for validation with optional image context."""
    validated_rows = []
    prompts = load_prompts()
    system_prompt = prompts["system_prompt"]
    user_prompt_template = prompts["user_prompt"]

    html_images = html_images or {}

    # Attach base64 image (if any) to each row by matching Page Number
    for row in rows:
        page_number = row.get("Page Number")

        # 🔐 Defensive: ensure it's hashable
        try:
            # If it's a list, grab the first element
            if isinstance(page_number, list):
                logging.warning(f"🧯 Page Number is a list: {page_number}")
                page_number = page_number[0] if page_number else None

            # If it's float like 1.0, convert to int
            elif isinstance(page_number, float):
                page_number = int(page_number)

            # Finally, try to access the image using the normalized key
            if html_images and page_number in html_images:
                image_bytes = html_images[page_number]
                row["page_image_base64"] = base64.b64encode(image_bytes).decode("utf-8")

        except TypeError as e:
            logging.error(f"❌ Unhashable Page Number: {page_number} in row: {row}")

    for batch in batch_rows(rows, BATCH_SIZE):
        user_prompt = user_prompt_template.format(data=json.dumps(batch, indent=2))
        response = run_prompt(system_prompt, user_prompt)
        
        try:
            response = response.strip()
            if response.startswith("```json"):
                response = response.strip("`").replace("json", "", 1).strip()
            elif response.startswith("```"):
                response = response.strip("`").strip()

            if not isinstance(response, str):
                validated_rows.append({"error": "Invalid LLM response type"})
                continue

            if not response.startswith("[") and not response.startswith("{"):
                validated_rows.append({"error": "Invalid JSON format from LLM"})
                continue

            parsed_response = json.loads(response)

            if isinstance(parsed_response, list) and all(isinstance(item, dict) and not item for item in parsed_response):
                continue

            validated_rows.extend(parsed_response)

        except json.JSONDecodeError as e:
            validated_rows.append({"error": f"Invalid JSON format from LLM: {str(e)}"})

    return validated_rows
 
def validate_taxonomy_with_llm(taxonomy_data):
    prompts = load_prompts()
    system_prompt = prompts.get("system_prompt_taxonomy", "")  # Use separate system prompt
    taxonomy_prompt = prompts["taxonomy"]
 
    try:
        # logging.info(f"TAXANOMY DATA:  {taxonomy_data}")
        user_prompt = taxonomy_prompt.format(data=json.dumps(taxonomy_data, indent=2))
        logging.info(f'HTML --> {user_prompt}')
 
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
 
def concept_label_filter(excel_rows, matched_taxonomy_blob_name):
    """Filter excel rows based on concept label match with taxonomy file."""
    try:
        taxonomy_bytes = get_blob_content("taxanomy", matched_taxonomy_blob_name)
        xls = pd.ExcelFile(io.BytesIO(taxonomy_bytes))
 
        if "Presentation" not in xls.sheet_names:
            logging.warning(f"'Presentation' sheet not found in {matched_taxonomy_blob_name}")
            return excel_rows, []
 
        presentation_df = pd.read_excel(xls, sheet_name="Presentation")
        if "Label" not in presentation_df.columns:
            logging.warning(f"'Label' column not found in Presentation sheet of {matched_taxonomy_blob_name}")
            return excel_rows, []
 
        labels_set = set(presentation_df["Label"].dropna().astype(str).str.strip())
 
        matched_rows = []
        unmatched_rows = []
 
        for row in excel_rows:
            concept = str(row.get("Concept Label", "")).strip()
            # logging.info(f"Checking CONCEPT LABEL FROM SILVER '{concept}'")
 
            if concept in labels_set:
                matched_rows.append(row)
            else:
                unmatched_rows.append(row)
 
 
        logging.info(f"✅ {len(matched_rows)} rows matched from {matched_taxonomy_blob_name}")
        # logging.info(f"{matched_rows} : MATCHED LABEL")
 
        logging.warning(f"⚠️ {len(unmatched_rows)} rows did not match in Presentation sheet from {matched_taxonomy_blob_name}")
        # logging.warning(f"⚠️ {unmatched_rows} : UNMATCHED LABEL")
 
        return matched_rows, unmatched_rows
 
    except Exception as e:
        logging.error(f"Failed concept_label_filter for {matched_taxonomy_blob_name}: {str(e)}")
        return excel_rows, []
   
def normalize_taxonomy_name(taxonomy_name: str) -> Tuple[str, str]:
    taxonomy_name = taxonomy_name.lower()
 
    if "frs 101" in taxonomy_name:
        taxonomy_type = "frs-101"
    elif "frs 102" in taxonomy_name:
        taxonomy_type = "frs-102"
    elif "ifrs" in taxonomy_name:
        taxonomy_type = "ifrs"
    else:
        taxonomy_type = taxonomy_name
 
    if any(keyword in taxonomy_name for keyword in ["ireland", "irish"]):
        jurisdiction = "ireland"
    elif any(keyword in taxonomy_name for keyword in ["uk", "frc", "united kingdom"]):
        jurisdiction = "uk"
    else:
        jurisdiction = taxonomy_name
 
    return taxonomy_type, jurisdiction
 
def _main_logic(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
 
    # 🔍 List all taxonomy files in taxonomy container
    taxonomy_blobs = list_blobs("taxanomy")
    taxonomy_blobs_list = list(taxonomy_blobs)  # convert iterable to list for reuse
    logging.info("📁 Listing blobs in 'taxanomy' container:")
    for blob in taxonomy_blobs_list:
        logging.info(f"🗂️ {blob.name}")
 
    req_body = req.get_json()
    selected_blobs = req_body.get("blobs", None)
    input_dates = req_body.get("selectedDates", [])
 
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
 
        blob_results = []
        all_periods = []
 
 
        for future in as_completed(futures):
            res = future.result()
            blob_results.append(res)
 
            # LLM input prep (leave as is)
            if res["taxonomy_data"]:
                taxonomy_data_to_validate.extend(res["taxonomy_data"])
            if res.get("statement_of_compliance_text"):
                taxonomy_data_to_validate.append({
                    "source": "html_statement_of_compliance",
                    "content": res["statement_of_compliance_text"]
                })
           
            if res["error"]:
                errors.append(res["error"])
            all_periods.extend(res.get("unique_periods", []))
 
        # first: validate taxonomy first
        # logging.warning(f"Taxonomy Name Extracted: {next((entry['SWL'] for entry in taxonomy_data_to_validate if entry.get('Filer Name') == 'Taxonomy Name'), None)}")
        logging.warning(f"Taxonomy Data to Validate: {taxonomy_data_to_validate}")
       
# extract taxonomy name dynamically regardless of structure
        taxonomy_name = None
        for row in taxonomy_data_to_validate:
            if row.get("Filer Name") == "Taxonomy Name":
                # Get the first value that is NOT 'Filer Name'
                taxonomy_name = next((v for k, v in row.items() if k != "Filer Name"), None)
                break
 
        logging.info(f"📘 Taxonomy Name Extracted: {taxonomy_name}")
 
        logging.info(f"HTML DATA ----> {taxonomy_data_to_validate}")
 
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
 
        # for res in blob_results:
        #     if res["excel_rows"]:
        #         validated_data.extend(validate_with_llm(res["excel_rows"]))
 
        # Dynamically match taxonomy_name to available files in taxanomy container
 
        matched_taxonomy_file = None
 
        if taxonomy_name:
            taxonomy_type, jurisdiction = normalize_taxonomy_name(taxonomy_name)
            logging.info(f"🔍 Normalized taxonomy_type: {taxonomy_type}, jurisdiction: {jurisdiction}")
 
            def is_valid_candidate(file):
                fname = file.name.lower()
                if jurisdiction.lower() in ["irish", "ireland"]:
                    return "ireland-frs-2023" in fname
                elif jurisdiction.lower() in ["uk", "frc", "united kingdom"]:
                    return "frc-2023" in fname
                return False
 
            valid_candidates = [b for b in taxonomy_blobs_list if is_valid_candidate(b)]
            best_score = 0
 
            for blob in valid_candidates:
                filename = blob.name.lower()
                score = 5 if taxonomy_type in filename else 0
                score += 5 - min(Levenshtein.distance(taxonomy_type, filename), 5)  # Fuzzy match on taxonomy_type
 
                if score > best_score:
                    best_score = score
                    matched_taxonomy_file = blob.name
 
 
        logging.warning(f"DHOOM MACHALE: {matched_taxonomy_file}")
 
       
        for res in blob_results:
            if res["excel_rows"]:
                matched_file = matched_taxonomy_file
                html_images = res.get("html_page_images", {})  # 🔥 grab the in-memory images

                if matched_file:
                    filtered_rows, unmatched_rows = concept_label_filter(res["excel_rows"], matched_file)
                    logging.info(f"LLM KO MATCHED CONCEPT LABELS BHEJRE --> {len(filtered_rows)}")
                    logging.info(f"HTML_IMAGES--->",{html_images})
                    
                    # validated_data.extend(validate_with_llm(filtered_rows, html_images))  # 🔥 pass html images

                    if unmatched_rows:
                        for row in unmatched_rows:
                            validated_data.append({
                                "Concept Label": row.get("Concept Label"),
                                "validation_result": [{ 
                                    "status": "FLAGGED FOR REVIEW",
                                    "reason": "Concept Label not found in matched taxonomy file"
                                }]
                            })
                else:
                    logging.warning(f"❌ No matched taxonomy file found for {res['blob_name']}. Sending all rows to LLM.")
                    validated_data.extend(validate_with_llm(res["excel_rows"], html_images))  # still pass images


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