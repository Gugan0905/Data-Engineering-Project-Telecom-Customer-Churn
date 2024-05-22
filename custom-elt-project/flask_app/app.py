import pg8000
from flask import Flask, request, render_template

app = Flask(__name__)

def get_db_connection():
    conn = pg8000.connect(
        host="destination_postgres",
        database="destination_db",
        user="postgres",
        password="secret",
        port=5432
    )
    return conn

@app.route('/')
def index():
    return render_template('survey.html')

@app.route('/submit', methods=['POST'])
def submit():
    data = request.form.to_dict()
    print("Survey Response - ")
    print(data)
    
    # Convert data to a list to match the order of the placeholders in the query
    data_list = [
        data.get('customerID'), data.get('count'), data.get('country'), data.get('state'), data.get('city'),
        data.get('zipCode'), data.get('latlong'), data.get('latitude'), data.get('longitude'), data.get('gender'),
        data.get('seniorCitizen_fl'), data.get('partner'), data.get('dependents'), data.get('tenure_months'),
        data.get('phoneService'), data.get('multipleLines'), data.get('internetService'), data.get('onlineSecurity'),
        data.get('onlineBackup'), data.get('deviceProtection'), data.get('techSupport'), data.get('streamingTV'),
        data.get('streamingMovies'), data.get('contract'), data.get('paperlessBilling'), data.get('paymentMethod'),
        data.get('monthlyCharges'), data.get('totalCharges'), data.get('churn_label'), data.get('churn_value'),
        data.get('churn_score'), data.get('cltv'), data.get('churn_reason')
    ]
    
    conn = get_db_connection()
    cursor = conn.cursor()

    insert_query = """
INSERT INTO telecom_customer_churn ("CustomerID", "Count", "Country", "State", "City", "Zip Code", "Lat Long", "Latitude", "Longitude", "Gender", "Senior Citizen", "Partner", "Dependents", "Tenure Months", "Phone Service", "Multiple Lines", "Internet Service", "Online Security", "Online Backup", "Device Protection", "Tech Support", "Streaming TV", "Streaming Movies", "Contract", "Paperless Billing", "Payment Method", "Monthly Charges", "Total Charges", "Churn Label", "Churn Value", "Churn Score", "CLTV", "Churn Reason") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    cursor.execute(insert_query, data_list)
    conn.commit()
    cursor.close()
    conn.close()
    return "Data submitted successfully"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
