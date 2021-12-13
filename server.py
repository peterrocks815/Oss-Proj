from flask import Flask, render_template, request, redirect, url_for, send_file
import os
import main

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('upload.html')


@app.route('/', methods=['POST'])
def upload_file():
    uploaded_file = request.files
    if ".csv" in uploaded_file['csv_file'].filename:
        uploaded_file['csv_file'].save("input/data.csv")
    if ".txt" in uploaded_file['txt_file'].filename:
        uploaded_file['txt_file'].save("input/schema.txt")
    if ".csv" not in uploaded_file['config_file'].filename and ".txt" not in uploaded_file['config_file'].filename and uploaded_file['config_file'].filename is not "":
        uploaded_file['config_file'].save("input/config")
    if os.path.isfile("input/data.csv") and os.path.isfile("input/schema.txt") and os.path.isfile(
            "input/config"):
        main.create_output("input/data.csv", "input/schema.txt", "input/config")
        return redirect(url_for('download_file'))
    return redirect(url_for('index'))


@app.route('/download')
def download_file():
    return render_template('download.html')


@app.route('/download/output', methods=['GET'])
def get_file():
    return send_file('output.zip')


if __name__ == '__main__':
    app.run(host="0.0.0.0")
