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
from string import Template


import logging, traceback

logging.getLogger().setLevel(logging.INFO)
 
try:

    logging.warning("module import: started")

    # ← keep your other imports/defs here

    logging.warning("module import: OK")

except Exception as e:

    logging.error("module import FAILED: %s\n%s", e, traceback.format_exc())

    raise

 

REQUIRED_CONTEXT_KEYS = [

    "Line Item Description",  # LID

    "Concept Label",

    "Comment Text",

    "Dimensions",

    "Tag Value",

]
 
 
# Define batch size (adjust based on LLM token limits)
BATCH_SIZE = 5
 
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

            # --- Column normalization (case/space/Unicode tolerant) ---

            def _canon(x: str) -> str:
                s = str(x or "")
                # normalize funny unicode quotes/dashes/spaces
                s = (s.replace("’", "'")
                    .replace("–", "-")
                    .replace("—", "-")
                    .replace("\u00A0", " "))
                s = re.sub(r"\s+", " ", s).strip().lower()
                return s
            alias_map = {"line item description":"Line Item Description",
                        "concept label":"Concept Label","comment text":"Comment Text",
                        "dimensions":"Dimensions","tag value":"Tag Value",}
            rename = {}
            for c in list(df_filing_details.columns):
                key = _canon(c)
                if key in alias_map:
                    rename[c] = alias_map[key]
            if rename:
                df_filing_details = df_filing_details.rename(columns=rename)
 
            logging.info("Filing Details columns (normalized): %s", list(df_filing_details.columns))
 
            # Extract relevant columns for LLM validation
            # df = df_filing_details[['Line Item Description', 'Concept Label', 'Comment Text','Dimensions','Tag Value']].dropna(how='all')
            # Extract relevant columns for LLM validation (tolerate missing columns)

            required_cols = ["Line Item Description", "Concept Label", "Comment Text", "Dimensions", "Tag Value"]
            available = [c for c in required_cols if c in df_filing_details.columns]
            df = df_filing_details[available].copy()  # select only what exists
        # add any missing columns as empty strings
            for c in required_cols:
                if c not in df.columns:
                    df[c] = ""
        # reorder to our canonical schema
            df = df[required_cols]
        # optional: drop rows that are entirely empty across our required columns
            df = df.replace("", pd.NA).dropna(how="all").fillna("")

 
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

def _clean(v: object) -> str:

    """Return '' for None/NaN, else a safe string."""

    try:

        if v is None:

            return ""

        # pd.isna handles numpy.nan and pandas NA types

        if pd.isna(v):  # pandas already imported as pd in your file

            return ""

    except Exception:

        pass

    return str(v)
 
 
def _normalize_llm_item_with_context(llm_obj: dict, src_row: dict) -> dict:

    """

    Keep all row context from the source row (Excel/filing).

    Use LLM only for Validation.status/reason.

    If LLM omits them, default to FLAGGED_FOR_REVIEW with a clear reason.

    Also convert NaN/None to '' so we never emit literal 'nan'.

    """

    llm_obj = llm_obj or {}
 
    validation = llm_obj.get("Validation") or {}

    status = (llm_obj.get("status") or validation.get("status") or "").strip()

    reason = (llm_obj.get("reason") or validation.get("reason") or "").strip()
 
    # Fallback if the model didn't give us a verdict

    if not status:

        status = "FLAGGED_FOR_REVIEW"

        if not reason:

            reason = "Model omitted a verdict; defaulted to review."
 
    return {

        "Line Item Description": _clean(src_row.get("Line Item Description")),

        "Concept Label":         _clean(src_row.get("Concept Label")),

        "Comment Text":          _clean(src_row.get("Comment Text")),

        "Dimensions":            _clean(src_row.get("Dimensions")),

        "Tag Value":             _clean(src_row.get("Tag Value")),

        "Validation": {

            "status": status,

            "reason": reason,

        },

    }

 


def validate_with_llm(rows):
    """
    Apply deterministic rules first, then send whatever is unresolved to the LLM.
    Output schema matches your local validate.py:
      keys: "Line Item Description", "Concept label", "Comment Text", "Dimensions", "Tag Value",
            "Validation": {"status": "<MATCH|MISSING_DATA|FLAGGED_FOR_REVIEW>", "reason": "..."}
    """
    # Accept None or a single dict safely

    if rows is None:

        rows = []

    elif not isinstance(rows, list):

        rows = [rows]

    validated_rows = []
    pass_through = []

    prompts = load_prompts()
    system_prompt = prompts["system_prompt"]
    user_prompt_template = prompts["user_prompt"]

    for idx, row in enumerate(rows):
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
            row_with_id = dict(row)          # shallow copy so we don't mutate the original
            row_with_id["__row_id"] = idx    # <-- attach stable id
            pass_through.append(row_with_id)
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
        row_with_id = dict(row) 
        row_with_id["__row_id"] = idx
        pass_through.append(row_with_id)
        continue

    # ---- Send unresolved rows to LLM (existing batching kept) ----
    # ---- Send unresolved rows to LLM (existing batching kept) ----

    for i in range(0, len(pass_through), BATCH_SIZE):
        raw_batch = pass_through[i:i+BATCH_SIZE]
        # Add a per-row id the model must echo back
        batch =[]
        for j, row in enumerate(raw_batch):
            r = dict(row)
            r["__row_id"] = j  # id is only within this batch
            batch.append(r)

        user_prompt = user_prompt_template.replace("{data}", json.dumps(batch, indent=2))
    # Call your AOAI wrapper
        response = run_prompt(system_prompt, user_prompt)

    # Make sure we have a string to parse
        if isinstance(response, str):
            resp_text = response
        else:
        # If your run_prompt returns an SDK object, try to pull text content
            try:
                resp_text = response.choices[0].message.content
            except Exception:

            # Last resort: stringify

                resp_text = str(response)
 
    # Clean common code-fence wrappers

        resp_text = resp_text.strip()

        if resp_text.startswith("```json"):

            resp_text = resp_text[len("```json"):].strip().strip("`").strip()

        elif resp_text.startswith("```"):

            resp_text = resp_text.strip("`").strip()
 
    # Basic sanity check

        if not (resp_text.startswith("[") or resp_text.startswith("{")):
            logging.error("LLM response is not valid JSON format: preview=%r", resp_text[:200])
            validated_rows.append({"error": "Invalid JSON format from LLM"})
            continue
        
        # Helpful preview so we can see what the model sent

        logging.info("LLM response preview: %r", resp_text[:300])
 
        # 1) Parse JSON safely

        try:

            parsed = json.loads(resp_text)

        except Exception as e:

            logging.error("json.loads failed: %s | preview=%r", e, resp_text[:300])

            validated_rows.append({"error": f"Invalid JSON from LLM: {str(e)}"})

            continue
 
        # 2) Normalize ANY valid shape into a list we can iterate

        if parsed is None:

            logging.error("LLM returned JSON null; skipping this batch. preview=%r", resp_text[:200])

            continue
 
        parsed_list = None

        if isinstance(parsed, list):

            parsed_list = parsed

        elif isinstance(parsed, dict):

            items = parsed.get("items")  # safe .get

            if isinstance(items, list):

                parsed_list = items

            else:

        # treat single object as list of one

                parsed_list = [parsed]

        else:

            logging.error("LLM returned unexpected JSON type %s; preview=%r", type(parsed), resp_text[:200])

            validated_rows.append({"error": "Unexpected JSON type from LLM"})

            continue
 
        by_id = {}
        if isinstance(parsed_list, list):
            for it in parsed_list:
                if isinstance(it, dict) and "__row_id" in it:
                    by_id[it["__row_id"]] = it
        if not by_id and isinstance(parsed_list, list):
            for pos, it in enumerate(parsed_list):
                if isinstance(it, dict):
                    by_id[pos] = it
        
        logging.info("parsed type=%s | parsed_list type=%s",type(parsed).__name__, type(parsed_list).__name__)
 
        for j in range(len(batch)):
            try:
                src = batch[j]
                row_id = src.get("__row_id", j)
                item = by_id.get(row_id)

                if not item:
                    single_user_prompt = user_prompt_template.replace("{data}", json.dumps([src], indent=2))
                    single_resp = run_prompt(system_prompt, single_user_prompt)
                    single_txt = single_resp if isinstance(single_resp, str) else getattr(single_resp.choices[0].message, "content", str(single_resp))
                    single_txt = single_txt.strip()
                    if single_txt.startswith("```json"):
                        single_txt = single_txt[len("```json"):].strip().strip("`").strip()
                    elif single_txt.startswith("```"):
                        single_txt = single_txt.strip("`").strip()

                    try:
                        single_parsed = json.loads(single_txt)
                        if isinstance(single_parsed, list) and single_parsed and isinstance(single_parsed[0], dict):
                            item = single_parsed[0]
                        elif isinstance(single_parsed, dict):
                            item = single_parsed
                    except Exception:
                        item = None
                if not item:
                    item = {"Validation": {
                        "status": "FLAGGED_FOR_REVIEW",
                        "reason": "Model omitted this row in batch and retry; defaulted."
                    }}
                normalized = _normalize_llm_item_with_context(item, src)

                if not all(normalized.get(k, "") for k in REQUIRED_CONTEXT_KEYS):
                    logging.warning("Context fields missing; normalized record: %r", normalized)

                validated_rows.append(normalized)

            except Exception as e:
                logging.error("Failed to process row %d: %s", j, e)
                validated_rows.append(_normalize_llm_item_with_context({"Validation": {"status": "FLAGGED_FOR_REVIEW",
                                "reason": f"Post-process error: {e}"}},
                                batch[j]
                    )
                )
                continue

    return validated_rows


def validate_taxonomy_with_llm(taxonomy_data):
    prompts = load_prompts()
    system_prompt = prompts.get("system_prompt_taxonomy", "")  # Use separate system prompt
    taxonomy_prompt = prompts["taxonomy"]
 
    try:
        # logging.info(f"TAXANOMY DATA:  {taxonomy_data}")
        user_prompt = Template(taxonomy_prompt).safe_substitute(data=json.dumps(taxonomy_data, indent=2))
 
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
        user_prompt = user_prompt_template
        user_prompt = user_prompt.replace("{periods}", json.dumps(unique_periods, indent=2))
        user_prompt = user_prompt.replace("{input_dates}", json.dumps(input_dates, indent=2))

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
        logging.info("main(): entered")
        return _main_logic(req)
    except Exception as e:
        import traceback
        logging.error("Fatal error in main: %s\n%s", e, traceback.format_exc())
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

 