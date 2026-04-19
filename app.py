from flask import Flask, request, jsonify, render_template
from sql2mongo.codegen.mongodb_generator import MongoDBGenerator
from sql2mongo.parser.sql_parser import get_parser
from sql2mongo.semantic.semantic_analyzer import SemanticAnalyzer
from pymongo import MongoClient
import psycopg2
import json
import os

app = Flask(__name__)

# ---------------- DB CONNECTIONS ---------------- #

# MongoDB
mongo_uri = os.getenv("MONGO_URI")
if not mongo_uri:
    raise Exception("MONGO_URI not set")

mongo_client = MongoClient(mongo_uri)
mongo_db = mongo_client["transpiler_db"]

# PostgreSQL
def get_pg_connection():
    return psycopg2.connect(
        dbname=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT")
    )

def run_sql(query):
    conn = get_pg_connection()
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description] if cur.description else []
    conn.close()
    return rows, columns

def run_mongo(collection, query, projection=None):
    return list(mongo_db[collection].find(query, projection))

# ---------------- ROUTES ---------------- #

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/schema")
def get_schema():
    try:
        with open("schema.json") as f:
            return jsonify(json.load(f))
    except Exception as e:
        return jsonify({"error": f"Schema error: {str(e)}"}), 500

@app.route("/run", methods=["POST"])
def run_query():
    try:
        # ✅ Safe JSON parsing
        data = request.get_json()

        if not data:
            return jsonify({"error": "No JSON received"}), 400

        sql = data.get("sql")
        schema = data.get("schema")

        if not sql or not schema:
            return jsonify({"error": "Missing sql or schema"}), 400

        print("SQL:", sql)

        # ---------------- Transpiler ---------------- #
        parser = get_parser()
        analyzer = SemanticAnalyzer(schema)
        generator = MongoDBGenerator()

        ast = parser.parse(sql)
        analyzer.validate_query(ast)

        mongo_data = generator.generate(ast)
        print("mongo_data:", mongo_data)
        collection = mongo_data["collection"]

        # ---------------- SQL execution ---------------- #
        try:
            sql_rows, sql_columns = run_sql(sql)
        except Exception as e:
            return jsonify({"error": f"Postgres error: {str(e)}"})

        # ---------------- Mongo execution ---------------- #
        try:
            if "filter" in mongo_data:
                mongo_result = run_mongo(
                    collection,
                    mongo_data["filter"],
                    mongo_data.get("projection")
                )
            else:
                mongo_result = list(
                    mongo_db[collection].aggregate(mongo_data["pipeline"])
                )
        except Exception as e:
            return jsonify({"error": f"Mongo error: {str(e)}"})

        # Clean Mongo output
        for doc in mongo_result:
            doc.pop("_id", None)

        sql_result_serializable = [list(row) for row in sql_rows]

        return jsonify({
            "mongo": mongo_data["string"],
            "columns": sql_columns,
            "sql_result": sql_result_serializable,
            "mongo_result": mongo_result
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
