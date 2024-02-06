from flask import Flask, request, jsonify
from flask_cors import CORS
from database import Database
import os


app = Flask(__name__)
CORS(app, resources={r'/*': {'origins':"*"}})

@app.route("/api/specimen/<predictive_id>", methods=["GET"])
def api_get_specimen_by_pred_id(predictive_id):
    predictive_id = predictive_id.replace("-", "/")
    return jsonify(
        db.get_samples_with_pred_id(predictive_id)
    )

if __name__ == "__main__":
    db = Database(
        os.environ["PSQL_NAME"],
        os.environ["PSQL_HOST"],
        os.environ["PSQL_PORT"],
        os.environ["PSQL_USER"],
        os.environ["PSQL_PSSWD"],
    )
    # Debug
    # app.run(debug=True)
    # Production?
    from waitress import serve

    serve(app, host="0.0.0.0", port=8081)
