from datetime import date
from flask import Flask
from utility import get_html
app = Flask(__name__)
def get_date():
    return str(date.today().year) + '_' + str(date.today().month) + '_' + str(date.today().day)

def get_html():
    a = get_date()
    index_html = '''<!DOCTYPE html>
    <html>
      <body>
        <p>As of:%s</p>
        <img src="https://s3.amazonaws.com/hardoopmapreduce/%s.png">
      </body>
    </html>''' % (a,a)
    return index_html

@app.route('/')
def main():
   return get_html()

if __name__ == '__main__':
    app.run(host = "0.0.0.0", port=80)
