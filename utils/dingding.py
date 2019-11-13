# -*- coding: utf-8 -*-
import requests

def dingding_alert(title, text):
    webhook = "https://oapi.dingtalk.com/robot/send?access_token=641146c60a468e0fa73b46286bc7a2fc0d72a2ce3664426fde66b59cec98feaa"
    headers = {'Content-Type': 'application/json'}
    message = """
    {{"msgtype": "markdown",
            "markdown": {{
              "title" :"{title}",
              "text": "【监控报警】{text}"
            }}}}
    """.format(
        title=title,
        text=text
    )
    requests.post(webhook, headers=headers, data=message)
