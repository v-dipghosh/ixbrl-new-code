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
            df = df_filing_details[['Line Item Description', 'Concept Label', 'Comment Text','Dimensions','Tag Value']].dropna(how='all')
 
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
                full_text = []

                # --------------------------

                # Statement of Compliance

                # --------------------------
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
        
                if content:
                    full_text.append("=== STATEMENT OF COMPLIANCE ===")
                    full_text.extend(content)
        
                # --------------------------
                # NOTES TO THE FINANCIAL STATEMENTS (all occurrences)
                # --------------------------

                notes_occurrences = []
                for p in soup.find_all("p"):
                    if "NOTES TO THE FINANCIAL STATEMENTS" in p.get_text(strip=True).upper():
                        notes_occurrences.append(p)
        
                for idx, start_tag in enumerate(notes_occurrences, start=1):
                    notes_content = []
                    current = start_tag
                    while current:
                        text = current.get_text(strip=True)

                        if any(stop in text.upper() for stop in ["ACCOUNTING POLICIES", "DIRECTORS", "INDEPENDENT AUDITOR"]):
                            break
                        if text:
                            notes_content.append(text)
                        current = current.find_next_sibling("p")

                    if notes_content:
                        full_text.append(f"=== NOTES TO FS occurrence {idx} ===")
                        full_text.extend(notes_content)
        
                # --------------------------
                # Factors affecting tax charge for the year
                # --------------------------
                tax_section = []
                start_tag = None
                for p in soup.find_all("p"):
                    if "FACTORS AFFECTING TAX" in p.get_text(strip=True).upper():
                        start_tag = p
                        break
                if start_tag:
                    current = start_tag
                    while current:
                        text = current.get_text(strip=True)
                        if any(stop in text.upper() for stop in ["NOTES TO THE", "DIRECTORS", "INDEPENDENT AUDITOR"]):
                            break
                        if text:
                            tax_section.append(text)
                        current = current.find_next_sibling("p")        
                if tax_section:
                    full_text.append("=== FACTORS AFFECTING TAX ===")
                    full_text.extend(tax_section)        
                # --------------------------
                # Save everything in one field
                # --------------------------
                result["statement_of_compliance_text"] = "\n".join(full_text)
        
            except Exception as e:
                result["error"] = f"Error extracting HTML content from {blob_name}: {str(e)}"
            
            #     soup = BeautifulSoup(blob_bytes.decode('utf-8', errors='ignore'), 'html.parser')
 
            #     start_tag = None
            #     for p in soup.find_all("p"):
            #         if "STATEMENT OF COMPLIANCE" in p.get_text(strip=True).upper():
            #             start_tag = p
            #             break
 
            #     content = []
            #     if start_tag:
            #         current = start_tag
            #         while current:
            #             text = current.get_text(strip=True)
            #             if text.startswith("2.") and "ACCOUNTING POLICIES" in text.upper():
            #                 break
            #             if text:
            #                 content.append(text)
            #             current = current.find_next_sibling("p")
 
            #     result["statement_of_compliance_text"] = "\n".join(content)
 
            # except Exception as e:
            #     result["error"] = f"Error extracting HTML content from {blob_name}: {str(e)}"
 
    except Exception as e:
        result["error"] = f"Error processing blob {blob_name}: {str(e)}"
 
    return result
 
def batch_rows(rows, batch_size):
    """Split rows into smaller batches."""
    for i in range(0, len(rows), batch_size):
        yield rows[i:i + batch_size]
 
# def validate_with_llm(rows):
#     """Send batches of rows to LLM for validation."""
#     validated_rows = []
#     prompts = load_prompts()  
#     system_prompt = prompts["system_prompt"]
#     user_prompt_template = prompts["user_prompt"]  
 
#     for batch in batch_rows(rows, BATCH_SIZE):
#         # Use the user prompt from backend instead of constructing it manually
#         user_prompt = user_prompt_template.format(data=json.dumps(batch, indent=2))
 
#         response = run_prompt(system_prompt, user_prompt)
#         try:
#             response = response.strip()
 
#             # Handle Markdown formatting from LLM like ```json ... ```
#             if response.startswith("```json"):
#                 response = response.strip("`").replace("json", "", 1).strip()
#             elif response.startswith("```"):
#                 response = response.strip("`").strip()
 
#             # Ensure it's still a string before proceeding
#             if not isinstance(response, str):
#                 logging.error("LLM response is not a valid string.")
#                 validated_rows.append({"error": "Invalid LLM response type"})
#                 continue
 
#             if not response.startswith("[") and not response.startswith("{"):
#                 logging.error("LLM response is not valid JSON format")
#                 validated_rows.append({"error": "Invalid JSON format from LLM"})
#                 continue
 
#             parsed_response = json.loads(response)
 
#             # Optional: skip if it's [{}] or [{}] * n
#             if isinstance(parsed_response, list) and all(isinstance(item, dict) and not item for item in parsed_response):
#                 logging.warning("Skipping empty [{}] response from LLM")
#                 continue
#             logging.info(f'ROW BY ROW VALIDATION --> : {parsed_response}')
#             validated_rows.extend(parsed_response)
 
#         except json.JSONDecodeError as e:
#             logging.error(f"JSON parsing error: {str(e)}")
#             validated_rows.append({"error": f"Invalid JSON format from LLM: {str(e)}"})
 
#     return validated_rows

# from utils.validators import (
#     has_url, has_common_word, looks_like_date, is_numeric_only
# )

def validate_with_llm(rows):
    """
    Apply deterministic rules first, then send whatever is unresolved to the LLM.
    Output schema matches your local validate.py:
      keys: "Line Item Description", "Concept label", "Comment Text", "Dimensions", "Tag Value",
            "Validation": {"status": "<MATCH|MISSING_DATA|FLAGGED_FOR_REVIEW>", "reason": "..."}
    """
    validated_rows = []
    pass_through = []

    prompts = load_prompts()
    system_prompt = prompts["system_prompt"]
    user_prompt_template = prompts["user_prompt"]

    for row in rows:
        concept = row.get("Concept Label") or ""
        lid     = row.get("Line Item Description") or ""
        dims    = row.get("Dimensions") or ""
        tag     = row.get("Tag Value")
        tag_str = "" if tag is None else str(tag)

        lid_present = bool(str(lid).strip())

        # ---------- A) LID present ----------
        if lid_present:
            # A1) any meaningful word overlap between LID and Concept → MATCH
            if has_common_word(lid, concept):
                validated_rows.append({
                    "Line Item Description": row.get("Line Item Description"),
                    "Concept Label": row.get("Concept Label"),
                    "Comment Text": row.get("Comment Text"),
                    "Dimensions": row.get("Dimensions"),
                    "Tag Value": tag,
                    "Validation": { "status": "MATCH", "reason": "Line Item Description matches Concept Label contextually." }
                })
                continue
            # else unresolved → send to LLM
            pass_through.append(row)
            continue

        # ---------- B) LID missing ----------
        # B1) Any URL in Dimensions → MATCH
        if has_url(dims):
            validated_rows.append({
                "Line Item Description": row.get("Line Item Description"),
                "Concept Label": row.get("Concept Label"),
                "Comment Text": row.get("Comment Text"),
                "Dimensions": row.get("Dimensions"),
                "Tag Value": tag,
                "Validation": { "status": "MATCH", "reason": "Concept Label matches Dimension contextually (dimension link present)." }
            })
            continue

        # B2) Concept ↔ Dimensions word overlap → MATCH
        if has_common_word(concept, dims):
            validated_rows.append({
                "Line Item Description": row.get("Line Item Description"),
                "Concept Label": row.get("Concept Label"),
                "Comment Text": row.get("Comment Text"),
                "Dimensions": row.get("Dimensions"),
                "Tag Value": tag,
                "Validation": { "status": "MATCH", "reason": "Concept Label matches Dimension contextually." }
            })
            continue

        # B3) Concept mentions date AND Tag looks like a date → MATCH
        if "date" in concept.lower() and looks_like_date(tag_str):
            validated_rows.append({
                "Line Item Description": row.get("Line Item Description"),
                "Concept Label": row.get("Concept Label"),
                "Comment Text": row.get("Comment Text"),
                "Dimensions": row.get("Dimensions"),
                "Tag Value": tag,
                "Validation": { "status": "MATCH", "reason": "Concept expects a date and Tag Value is a date." }
            })
            continue

        # B4) Concept ↔ Tag overlap → MATCH
        if has_common_word(concept, tag_str):
            validated_rows.append({
                "Line Item Description": row.get("Line Item Description"),
                "Concept Label": row.get("Concept Label"),
                "Comment Text": row.get("Comment Text"),
                "Dimensions": row.get("Dimensions"),
                "Tag Value": tag,
                "Validation": { "status": "MATCH", "reason": "Concept Label matches Tag Value contextually." }
            })
            continue

        # B5) Tag numeric-only with missing LID → MISSING_DATA
        if is_numeric_only(tag_str):
            validated_rows.append({
                "Line Item Description": row.get("Line Item Description"),
                "Concept Label": row.get("Concept Label"),
                "Comment Text": row.get("Comment Text"),
                "Dimensions": row.get("Dimensions"),
                "Tag Value": tag,
                "Validation": { "status": "MISSING_DATA", "reason": "Numeric-only Tag Value with missing LID." }
            })
            continue

        # Otherwise → LLM
        pass_through.append(row)

    # ---- Send unresolved rows to LLM (existing batching kept) ----
    for i in range(0, len(pass_through), BATCH_SIZE):
        batch = pass_through[i:i+BATCH_SIZE]
        user_prompt = user_prompt_template.format(data=json.dumps(batch, indent=2))
        response = run_prompt(system_prompt, user_prompt)

        try:
            resp = response.strip()
            if resp.startswith("```json"): resp = resp.strip("`").replace("json", "", 1).strip()
            elif resp.startswith("```"):   resp = resp.strip("`").strip()
            if not isinstance(resp, str) or not (resp.startswith("[") or resp.startswith("{")):
                logging.error("LLM response is not valid JSON format")
                validated_rows.append({"error": "Invalid JSON format from LLM"})
                continue

            parsed = json.loads(resp)
            if isinstance(parsed, list):
                validated_rows.extend(parsed)
            else:
                validated_rows.append(parsed)

        except json.JSONDecodeError as e:
            logging.error(f"JSON parsing error: {str(e)}")
            validated_rows.append({"error": f"Invalid JSON format from LLM: {str(e)}"})

    return validated_rows


def validate_taxonomy_with_llm(taxonomy_data):
    prompts = load_prompts()
    system_prompt = prompts.get("system_prompt_taxonomy", "")  # Use separate system prompt
    taxonomy_prompt = prompts["taxonomy"]
 
    try:
        # logging.info(f"TAXANOMY DATA:  {taxonomy_data}")
        user_prompt = taxonomy_prompt.format(data=json.dumps(taxonomy_data, indent=2))
        # logging.info(f'HTML --> {user_prompt}')
 
        response = run_prompt(system_prompt, user_prompt).strip()
        logging.info(f'TAXANOMY LLM RESPONSE:{response}')
 
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
 
# def concept_label_filter(excel_rows, matched_taxonomy_blob_name):
#     """Filter excel rows based on concept label match with taxonomy file."""
#     try:
#         taxonomy_bytes = get_blob_content("taxanomy", matched_taxonomy_blob_name)
#         xls = pd.ExcelFile(io.BytesIO(taxonomy_bytes))
 
#         if "Presentation" not in xls.sheet_names:
#             logging.warning(f"'Presentation' sheet not found in {matched_taxonomy_blob_name}")
#             return excel_rows, []
 
#         presentation_df = pd.read_excel(xls, sheet_name="Presentation")
#         if "Label" not in presentation_df.columns:
#             logging.warning(f"'Label' column not found in Presentation sheet of {matched_taxonomy_blob_name}")
#             return excel_rows, []
 
#         labels_set = set(presentation_df["Label"].dropna().astype(str).str.strip())
 
#         matched_rows = []
#         unmatched_rows = []
 
#         for row in excel_rows:
#             concept = str(row.get("Concept Label", "")).strip()
#             # logging.info(f"Checking CONCEPT LABEL FROM SILVER '{concept}'")
 
#             if concept in labels_set:
#                 matched_rows.append(row)
#             else:
#                 unmatched_rows.append(row)
 
 
#         logging.info(f"✅ {len(matched_rows)} rows matched from {matched_taxonomy_blob_name}")
#         # logging.info(f"{matched_rows} : MATCHED LABEL")
 
#         logging.warning(f"⚠️ {len(unmatched_rows)} rows did not match in Presentation sheet from {matched_taxonomy_blob_name}")
#         # logging.warning(f"⚠️ {unmatched_rows} : UNMATCHED LABEL")
 
#         return matched_rows, unmatched_rows
 
#     except Exception as e:
#         logging.error(f"Failed concept_label_filter for {matched_taxonomy_blob_name}: {str(e)}")
#         return excel_rows, []
    

from utils.validators import (
    has_url, has_common_word, looks_like_date, is_numeric_only,
    normalize_for_lookup, _close_enough
)


def concept_label_filter(excel_rows, matched_taxonomy_blob_name):
    """Filter rows by checking if Concept Label exists (exact/alias/containment/fuzzy) in taxonomy Presentation sheet."""
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

        labels = [str(x).strip() for x in presentation_df["Label"].dropna().astype(str).tolist()]
        labels_norm = {normalize_for_lookup(x): x for x in labels}

        # Inject alias group if the canonical exists (mirrors your local validate.py logic)
        canonical = "Equity / share capital and reserves"
        aliases = ["Equity", "Equity share capital and reserves"]
        c_norm = normalize_for_lookup(canonical)
        if c_norm in labels_norm:
            for a in aliases:
                labels_norm[normalize_for_lookup(a)] = labels_norm[c_norm]

        matched_rows, unmatched_rows = [], []

        for row in excel_rows:
            concept = str(row.get("Concept Label", "") or "").strip()
            if not concept:
                unmatched_rows.append(row); continue

            c_norm = normalize_for_lookup(concept)

            # 1) direct/alias hit
            if c_norm in labels_norm:
                matched_rows.append(row); continue

            # 2) containment (equity in equity share capital and reserves, etc.)
            if any(c_norm in k or k in c_norm for k in labels_norm.keys()):
                matched_rows.append(row); continue

            # 3) fuzzy
            if any(_close_enough(c_norm, k) for k in labels_norm.keys()):
                matched_rows.append(row)
            else:
                unmatched_rows.append(row)

        logging.info(f"✅ {len(matched_rows)} rows matched in Presentation (incl. fuzzy/aliases)")
        logging.warning(f"⚠️ {len(unmatched_rows)} rows unmatched in Presentation")
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
        # logging.warning(f"Taxonomy Data to Validate: {taxonomy_data_to_validate}")
       
        # extract taxonomy name dynamically regardless of structure
        taxonomy_name = None
        for row in taxonomy_data_to_validate:
            if row.get("Filer Name") == "Taxonomy Name":
                # Get the first value that is NOT 'Filer Name'
                taxonomy_name = next((v for k, v in row.items() if k != "Filer Name"), None)
                break
 
        logging.warning(f"📘 Taxonomy Name Extracted: {taxonomy_name}")
 
        logging.info(f"DATA SENT FOR TAXANOMY VALIDATION----> {taxonomy_data_to_validate}")
 
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

        logging.warning(f"MATCHED TAXANOMY FILE: {matched_taxonomy_file}")
       
        for res in blob_results:
            if res["excel_rows"]:
                # matched_file = "FRC-2023-v1.0.1-FRS-101.xlsx"
                matched_file = matched_taxonomy_file
                if matched_file:
                    filtered_rows, unmatched_rows = concept_label_filter(res["excel_rows"], matched_file)
                    # logging.info(f"MATCHED TAXANOMY FILE -----> {matched_taxonomy_file}")
                    logging.info(f"LLM KO MATCHED CONCEPT LABELS BHEJRE --> {len(filtered_rows)}")
                    validated_data.extend(validate_with_llm(filtered_rows))
 
                    if unmatched_rows:
                        # logging.warning(f"⚠️ {len(unmatched_rows)} unmatched Concept Labels in {res['blob_name']}")
                                        # Add unmatched concept labels with validation message
                        # for row in unmatched_rows:
                        #     validated_data.append({
                        #         "Concept Label": row.get("Concept Label"),
                        #         "validation_result": [{ "status": "FLAGGED FOR REVIEW","reason": "Concept Label not found in matched taxonomy file"}]
                        #     })
                        for row in unmatched_rows:
                            validated_data.append({
                                "Line Item Description": row.get("Line Item Description"),
                                "Concept Label": row.get("Concept Label"),
                                "Comment Text": row.get("Comment Text"),
                                "Dimensions": row.get("Dimensions"),
                                "Tag Value": row.get("Tag Value"),
                                "Validation": {
                                "status": "FLAGGED_FOR_REVIEW",
                                "reason": "Concept Label not found in matched taxonomy file"}
                            })

 
                else:
                    logging.warning(f"❌ No matched taxonomy file found for {res['blob_name']}. Sending all rows to LLM.")
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