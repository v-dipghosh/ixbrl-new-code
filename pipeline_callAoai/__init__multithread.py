import azure.functions as func
import logging
from utils.prompts import load_prompts
from utils.blob_functions import get_blob_content, write_to_blob
from utils.azure_openai import run_prompt
import io
import os
import pandas as pd
from PyPDF2 import PdfReader
from bs4 import BeautifulSoup
from azure.storage.blob import BlobServiceClient
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

def validate_filing_row(row, system_prompt, user_prompt):
    filing_row_json = row.to_dict()
    user_input = (
        f"{user_prompt}\n\n"
        f"Excel Filing Row: {json.dumps(filing_row_json, indent=2)}\n\n"
    )
    try:
        llm_result = run_prompt(system_prompt, user_input)
        try:
            return json.loads(llm_result)  
        except Exception:
            return llm_result  
    except Exception as e:
        return f"LLM Error: {str(e)}"

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    try:
        req_body = req.get_json()
        # Get the list of blobs sent from the frontend (if any)
        selected_blobs = req_body.get("blobs", None)
        
        if not selected_blobs:
            return func.HttpResponse(
                json.dumps({"error": "No blobs provided."}),
                status_code=400,
                mimetype="application/json"
            )
        
        processed_files = []
        errors = []
        
        # Loop through each blob provided

        excel_content = None
        html_content = None
        for blob in selected_blobs:
            blob_name = blob.get("name")
            container_name = blob.get("container", "silver")  # Default to 'silver' if not provided
            
            if not blob_name:
                errors.append("Blob is missing the 'name' property.")
                continue
            
            # Disallow processing if the blob comes from a disallowed container
            if container_name == "bronze":
                logging.warning(f"Skipping blob from the bronze container: {blob_name}")
                errors.append(f"Processing blobs from the 'bronze' container is not allowed: {blob_name}")
                continue
            
            logging.info(f"Processing blob: {blob_name} from container: {container_name}")
            
            # Step 1: Get the content of the specified blob (expects a .txt file)
            # try:
            #     content = get_blob_content(container_name, blob_name).decode('utf-8')
            # except Exception as e:
            #     error_msg = f"Error getting content for blob {blob_name}: {str(e)}"
            #     logging.error(error_msg)
            #     errors.append(error_msg)
            #     continue
           

            try:
                blob_bytes = get_blob_content(container_name, blob_name)  # returns bytes
                ext = os.path.splitext(blob_name)[1].lower()

                if ext == '.csv':
                    content = pd.read_csv(io.BytesIO(blob_bytes))  # DataFrame

                elif ext == '.xlsx':
                    xls = pd.ExcelFile(io.BytesIO(blob_bytes))
                    # sheet_json_data = {}

                    # # Only process these sheets
                    # sheets_to_read = ["Filing Information", "Free Selection Comments", "Filing Details"]

                    # for sheet_name in sheets_to_read:
                    #     if sheet_name in excel_file.sheet_names:
                    #         df = excel_file.parse(sheet_name)
                    #         df_limited = df.head(10)  # limit to first 10 rows
                    #         sheet_json_data[sheet_name] = json.loads(df_limited.to_json(orient='records'))
                    #     else:
                    #         logging.warning(f"Sheet '{sheet_name}' not found in blob {blob_name}")

                    # 1. Filing Information
                    df_info = pd.read_excel(xls, sheet_name='Filing Information').dropna(how='all')
                    json_info = df_info.head(5).to_dict(orient='records')

                    # 2. Free Selection Comments
                    df_comments = pd.read_excel(xls, sheet_name='Free Selection Comments')
                    df_comments = df_comments[['Document Value', 'Comment Text']].dropna(how='all')
                    json_comments = df_comments.head(5).to_dict(orient='records')

                    # 3. Filing Details
                    df_filing = pd.read_excel(xls, sheet_name='Filing Details')
                    df_filing = df_filing[['Concept Label', 'Document Value', 'Tag Value']].dropna(how='all')
                    json_filing = df_filing.head(5).to_dict(orient='records')

                    df_date = pd.read_excel(xls, sheet_name='Filing Details')
                    periods = df_date['Period']
                    periods_clean = periods.dropna().astype(str).str.strip()
                    periods_clean = periods_clean[periods_clean != ""]
                    
                    # Extract unique dates
                    unique_dates = sorted(periods_clean.unique())
                    unique_dates

                    # Combine into sectioned format
                    excel_content = {
                        # "Filing Information": json_info,
                        # "Dates": unique_dates,
                        "Free Selection Comments": json_comments,
                        "Filing Details": json_filing }

                    excel_content = excel_content
                    logging.info(f'EXCEL EXTRACTION : {excel_content}')


                # elif ext == '.xlsx':
                #     content = pd.read_excel(io.BytesIO(blob_bytes))  # DataFrame

                elif ext == '.pdf':
                    reader = PdfReader(io.BytesIO(blob_bytes))
                    content = ''
                    for page in reader.pages:
                        content += page.extract_text()

                elif ext in ['.html', '.htm']:
                    soup = BeautifulSoup(blob_bytes.decode('utf-8'), 'html.parser')
                    # html_content  = soup.get_text()

                    # blob_bytes = get_blob_content(container_name, blob_name)  # returns bytes
                    # html_content = blob_bytes.decode("utf-8")
                    
                    # --- Parse HTML and Extract  ---
                    # soup = BeautifulSoup(html_content, "html.parser")
                    divs = soup.find_all("div", class_="page")
                    divs_html = ''.join(str(div) for div in divs)
                    
                    # --- Reparse only page divs ---
                    soup = BeautifulSoup(divs_html, "html.parser")
                    pages = soup.find_all("div", class_="page")
                    
                    pages_data = []
                    
                    for idx, page in enumerate(pages):
                        page_id = page.get("id")
                        try:
                            page_no = int(page_id) if page_id and page_id.isdigit() else idx + 1
                        except:
                            page_no = idx + 1
                    
                        text = page.get_text(separator=" ", strip=True)
                    
                        tags_list = []
                        for tag in page.find_all():
                            if tag.name and tag.name.startswith("ix:"):
                                tag_key = tag.get("name")
                                tag_value = tag.get_text(strip=True)
                                if tag_key:
                                    tag_entry = {
                                        "name": tag_key,
                                        "value": tag_value
                                    }
                                    if tag.has_attr("unitref"):
                                        tag_entry["unitRef"] = tag["unitref"]
                                    if tag.has_attr("decimals"):
                                        tag_entry["decimals"] = tag["decimals"]
                    
                                    dimensions = {
                                        attr_key: attr_val
                                        for attr_key, attr_val in tag.attrs.items()
                                        if "dimension" in attr_key.lower()
                                    }
                                    if dimensions:
                                        tag_entry["dimension"] = dimensions
                    
                                    tags_list.append(tag_entry)
                    
                        if tags_list:
                            pages_data.append({
                                "page_no": page_no,
                                "raw_text": text,
                                "tags": tags_list
                            })
                    
                    # --- Print JSON ---
                    html_content = json.dumps(pages_data, indent=2)
                    logging.info(f'HTML eXTRACTION: {html_content}')

                else:
                    content = blob_bytes.decode('utf-8')  # fallback for plain text or .txt

            except Exception as e:
                error_msg = f"Error getting content for blob {blob_name}: {str(e)}"
                logging.error(error_msg)
                errors.append(error_msg)
                continue

            # Step 2: Load Prompts
            try:
                logging.info("Loading Prompts")
                prompts = load_prompts()
                system_prompt = prompts["system_prompt"]
                user_prompt = prompts["user_prompt"]
            except Exception as e:
                error_msg = f"Error loading prompts for blob {blob_name}: {str(e)}"
                logging.error(error_msg)
                errors.append(error_msg)
                continue
            
# -----------------------------------------------------------------------
            # Combinig the html + excel tags before sending to LLM
            
            # html_data = json.loads(result_json)
            # content["HTML Pages"] = html_data

            # This is the part that would be going to the LLM
            # full_user_prompt = user_prompt + content
# -------------------------------------------------------------------------
            # Build structured prompt with JSON sheets if it's Excel
            
            # if isinstance(content, dict):
            #     full_user_prompt = user_prompt + "\n\n"
            #     for sheet_name, rows in content.items():
            #         # Take only first 10 records from each sheet
            #         limited_rows = rows[:300]
            #         full_user_prompt += f"### Sheet: {sheet_name}\n"
            #         full_user_prompt += json.dumps(limited_rows, indent=2) + "\n\n"
            # else:
            #     full_user_prompt = user_prompt + content

            # if excel_content and html_content:
            #     combined_content = {
            #         "Excel Data": excel_content,
            #         "HTML Text": html_content
            #     }
            # elif excel_content:
            #     combined_content = excel_content
            # elif html_content:
            #     combined_content = html_content
            # else:
            #     combined_content = None  # or handle accordingly

            combined_content = {
                    "Excel Data": excel_content,
                    "HTML Text": html_content
                }

            # Convert combined content to string prompt
            if isinstance(combined_content, dict):
                full_user_prompt = user_prompt + "\n\n" + json.dumps(combined_content, indent=2)
            else:
                full_user_prompt = user_prompt + "\n\n" + str(combined_content)
            logging.info(f'THIS IS PASSED TO LLM : {combined_content}')
            
            # Then call the LLM with this prompt
            # response_content = run_prompt(system_prompt, full_user_prompt)
            # Then call the LLM with this prompt using ThreadPoolExecutor
            responses = []

            def call_llm(line):
                prompt = user_prompt + "\n\n" + line
                return run_prompt(system_prompt, prompt)

            if isinstance(combined_content, dict):
                text_lines = json.dumps(combined_content, indent=2).splitlines()
            else:
                text_lines = str(combined_content).splitlines()

            with ThreadPoolExecutor(max_workers=8) as executor:  # adjust workers as needed
                future_to_line = {executor.submit(call_llm, line): line for line in text_lines if line.strip()}
                for future in as_completed(future_to_line):
                    try:
                        result = future.result()
                        responses.append(result)
                    except Exception as e:
                        logging.error(f"Error in LLM call for line: {e}")
                        responses.append(f"Error: {str(e)}")

            # Combine all responses into a single string or JSON list
            # response_content = "\n".join(responses)

            response_content = json.dumps(responses, indent=2)




            
            # Step 3: Call OpenAI to generate response
            # try:
            #     response_content = run_prompt(system_prompt, full_user_prompt)
            # except Exception as e:
            #     error_msg = f"Error running prompt for blob {blob_name}: {str(e)}"
            #     logging.error(error_msg)
            #     errors.append(error_msg)
            #     continue
            
            # Clean up JSON response if necessary
            if response_content.startswith('```json') and response_content.endswith('```'):
                response_content = response_content.strip('`')
                response_content = response_content.replace('json', '', 1).strip()
            
            json_bytes = response_content.encode('utf-8')
            
            # Step 4: Write the response to a blob in the 'gold' container
            try:
                sourcefile = os.path.splitext(os.path.basename(blob_name))[0]
                write_to_blob("gold", f"{sourcefile}-output.json", json_bytes)
                processed_files.append(blob_name)
            except Exception as e:
                error_msg = f"Error writing output for blob {blob_name}: {str(e)}"
                logging.error(error_msg)
                errors.append(error_msg)
                continue
        
        # Prepare the response payload
        response_data = {
            "processedFiles": processed_files,
            "errors": errors,
            "status": "completed" if not errors else "completed_with_errors"
        }
        
        status_code = 200 if not errors else 500
        
        return func.HttpResponse(
            json.dumps(response_data),
            status_code=status_code,
            mimetype="application/json"
        )
    
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )