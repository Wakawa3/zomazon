# coding: utf-8

import subprocess
import json
from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse
from urllib.parse import parse_qs

class class1(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)

        # GET /
        if parsed.path.find('products') == -1:
            self.send_response(200)
            self.end_headers()
            
            with open('index.html', 'r', encoding='UTF-8') as f:
                html = f.read()

            self.wfile.write(html.encode())
            return

        # GET /products
        result = ""

        try:
            raw_result = str(subprocess.run('./gradlew run --args="GetItemList"', shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True))
            
            left = raw_result.find('{', 0)

            if left == -1:
                self.send_response(500)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                text = "error"
                self.wfile.write(text.encode())
                return
            
            result = "["
            
            while left != -1:
                right_1 = raw_result.find('}', left)
                right = raw_result.find('}', right_1+1)
                result += raw_result[left:right+1]
                left = raw_result.find('{', right + 1)
                if left != -1:
                    result += ","
            
            result += "]"
            #result = result.replace(r'/', '')
            
        except subprocess.CalledProcessError:
            print('外部プログラムの実行に失敗しました', file=sys.stderr)
        
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        responseBody = result

        print('send products')

        self.wfile.write(responseBody.encode())
        
        

    def do_POST(self):
        parsed = urlparse(self.path)
        print(parsed.path)
        path = str(parsed.path)

        item_id = path[10:]
        
        try:
            command = './gradlew run --args="PlaceOrder 1 ' + item_id + ':1"'
            
            order_result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            
            if str(order_result).find('order_id') == -1:
                self.send_response(500)
            else:
                self.send_response(200)

            self.send_header('Content-type', 'application/json')
            self.end_headers()



        except subprocess.CalledProcessError:
            print('外部プログラムの実行に失敗しました', file=sys.stderr)
        
        response = ""
        self.wfile.write(response.encode())

ip = '127.0.0.1'
port = 8765

server = HTTPServer((ip, port), class1)

server.serve_forever()