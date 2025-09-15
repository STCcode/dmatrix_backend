from flask import request, make_response
from werkzeug.utils import secure_filename
from datetime import datetime
import os
import queries, middleware, responses
from pdf_parser import process_pdf   # <-- your PDF extraction file

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def upload_and_save():
    try:
        if request.method != 'POST':
            return make_response({"error": "Method not allowed"}, 405)

        if 'files' not in request.files:
            return make_response({"error": "No files uploaded"}, 400)

        files = request.files.getlist('files')
        if not files:
            return make_response({"error": "Empty files list"}, 400)

        category = request.form.get("category")
        subcategory = request.form.get("subcategory")
        if not category or not subcategory:
            return make_response({"error": "Category and Subcategory are required"}, 400)

        inserted_records = []
        now = datetime.now()

        for file in files:
            filename = secure_filename(file.filename)

            # Parse PDF → structured JSON
            broker, json_data = process_pdf(file, category, subcategory)

            for row in json_data:
                entity = row.get("entityTable", {})
                action = row.get("actionTable", {})

                # Insert entity if not already existing
                entityid = entity.get("entityid")
                if not entityid:
                    entity_fields = (
                        entity.get("scripname"),
                        entity.get("scripcode"),
                        entity.get("benchmark"),
                        entity.get("category"),
                        entity.get("subcategory"),
                        entity.get("nickname"),
                        entity.get("isin"),
                        now
                    )
                    entityid = queries.insert_entity_return_id(entity_fields)

                # Insert action
                action_fields = (
                    action.get("scrip_code"),
                    action.get("mode"),
                    action.get("order_type"),
                    action.get("scrip_name"),
                    action.get("isin"),
                    action.get("order_number"),
                    action.get("folio_number"),
                    action.get("nav"),
                    action.get("stt"),
                    action.get("unit"),
                    action.get("redeem_amount"),
                    action.get("purchase_amount"),
                    action.get("net_amount"),
                    action.get("stamp_duty"),
                    now,
                    entityid,
                    action.get("order_date"),
                    action.get("sett_no"),
                )
                queries.auto_action_table(action_fields)

                inserted_records.append({
                    "entityid": entityid,
                    "order_number": action.get("order_number")
                })

        return make_response(
            middleware.exs_msgs(inserted_records, responses.insert_200, '1020200'),
            200
        )

    except Exception as e:
        print("Error in upload_and_save:", e)
        return make_response(
            middleware.exe_msgs(responses.insert_501, str(e.args), '1020500'),
            500
        )
