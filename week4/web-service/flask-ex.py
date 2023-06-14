from flask import Flask


# create the Flask app
app = Flask("web-service")

# define a route
@app.route("/ping", methods=["GET"])
def ping():
    return "PONG"

if __name__ == "__main__":
    app.run(debug=True, host='localhost', port=5000) # run app in debug mode on port 5000
