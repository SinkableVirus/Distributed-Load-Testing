from http.server import BaseHTTPRequestHandler, HTTPServer
import time

hostName = "localhost"
serverPort = 8080

requests = 0
responses = 0

class ServerNode(BaseHTTPRequestHandler):
    
    def do_GET(self):
        global requests
        global responses
        if self.path == "/metrics":
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            print(requests, responses)
            self.wfile.write(bytes("<html>", "utf-8"))
            self.wfile.write(bytes("<body>", "utf-8"))
            self.wfile.write(bytes("<p>Requests: %s</p>" % requests, "utf-8"))
            self.wfile.write(bytes("<p>Responses: %s</p>" % responses, "utf-8"))
            self.wfile.write(bytes("</body></html>", "utf-8"))
        
        if self.path == "/ping":
            requests += 1
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            responses += 1
            print(self.request)



if __name__ == "__main__":        
    webServer = HTTPServer((hostName, serverPort), ServerNode)
    print("Server started http://%s:%s" % (hostName, serverPort))

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    webServer.server_close()
    print("Server stopped.")