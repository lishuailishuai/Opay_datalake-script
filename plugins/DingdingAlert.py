import requests


class DingdingAlert:
    url = None

    def __init__(self, url):
        self.url = url

    def send(self, content):
        if not self.url:
            raise Exception("url 地址不能为空")

        json = {
            'msgtype': 'text',
            'text': {
                'content': content
            }
        }
        requests.post(url=self.url, json=json)

    def markdown_send(self, title,content):
        if not self.url:
            raise Exception("url 地址不能为空")

        json = {
            "msgtype": "markdown",
            "markdown": {
                "title":title,
                "text": content
            }

        }
        
        requests.post(url=self.url, json=json)
