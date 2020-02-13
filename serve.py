from flask import Flask, request, jsonify
import fetch

app = Flask(__name__)

@app.route('/')
def station_details():
    response = jsonify({
        'data': fetch.todays()
    })
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

@app.route('/history')
def aggregated_past():
    response = jsonify({
        'data': fetch.agg()
    })
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=12434)