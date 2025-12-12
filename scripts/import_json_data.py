import json
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# ---------------------------------------------------------
# CONNECT TO SUPABASE DATABASE
# ---------------------------------------------------------
# You must have:
#   export DATABASE_URL="postgres://...."
#
DATABASE_URL = os.getenv("DATABASE_URL")


# ---------------------------------------------------------
# Helper Function: Get row or insert and return id
# This avoids duplicates and keeps the code simple.
# ---------------------------------------------------------
def get_or_create(cur, table, lookup_fields, insert_fields):
    """
    lookup_fields: dict of fields used to find an existing row
    insert_fields: dict of fields used only when inserting a new row
    """

    # Build a simple WHERE clause such as "name = %s"
    where = " AND ".join([f"{k} = %s" for k in lookup_fields.keys()])
    values = list(lookup_fields.values())

    # 1. Try to find an existing row
    cur.execute(f"SELECT id FROM {table} WHERE {where}", values)
    row = cur.fetchone()

    if row:
        return row["id"]

    # 2. If not found, insert a new row
    all_fields = {**lookup_fields, **insert_fields}
    columns = list(all_fields.keys())
    placeholders = ", ".join(["%s"] * len(columns))

    cur.execute(
        f"""
        INSERT INTO {table} ({", ".join(columns)})
        VALUES ({placeholders})
        RETURNING id
        """,
        list(all_fields.values())
    )
    new_row = cur.fetchone()
    return new_row["id"]


# ---------------------------------------------------------
# MAIN IMPORT LOGIC
# ---------------------------------------------------------
def import_sign_database(json_path):

    # 1. Load the JSON into memory
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # 2. Connect to the database
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    cur = conn.cursor()

    # -----------------------------------------------------
    # The JSON structure:
    # {
    #   "Regulatory Signs": {
    #        "R1 series": [ {sign...}, {...} ],
    #        "R2 series": [...]
    #   },
    #   "Warning Signs": { ... }
    # }
    # -----------------------------------------------------

    for category_name, series_dict in data.items():

        # Insert category
        category_id = get_or_create(
            cur,
            "categories",
            {"name": category_name},
            {"description": f"{category_name} (imported)"}
        )

        # Loop each series under this category
        for series_name, signs_list in series_dict.items():

            # Extract the short code (e.g. "R1" from "R1 series")
            series_code = series_name.replace(" series", "").strip()

            series_id = get_or_create(
                cur,
                "series",
                {"category_id": category_id, "series_code": series_code},
                {"description": series_name}
            )

            # Loop all signs in the series
            for sign in signs_list:
                designation = sign["sign_designation"]
                sign_name = sign["sign_name"]
                notes = sign.get("notes", "")

                # Insert the sign record
                sign_id = get_or_create(
                    cur,
                    "signs",
                    {"designation": designation},
                    {
                        "series_id": series_id,
                        "name": sign_name,
                        "notes": notes
                    }
                )

                # Insert file references
                file_fields = {
                    "png_file_link": "png",
                    "svg_file_link": "svg",
                    "pdf_file_link": "pdf_full",
                    "layout_file_link": "layout",
                    "png_file": "png_page",
                    "pdf_file": "pdf_page"
                }

                for json_field, file_type in file_fields.items():
                    file_path = sign.get(json_field)
                    if file_path and file_path.strip():
                        cur.execute(
                            """
                            INSERT INTO sign_files (sign_id, file_type, storage_url)
                            VALUES (%s, %s, %s)
                            """,
                            (sign_id, file_type, file_path)
                        )

    conn.commit()
    cur.close()
    conn.close()

    print("\nImport complete! Database is populated.\n")


# ---------------------------------------------------------
# RUN SCRIPT
# ---------------------------------------------------------
if __name__ == "__main__":
    json_path = os.environ.get("JSON_PATH", "database_with_pages.json")
    import_sign_database(json_path)
