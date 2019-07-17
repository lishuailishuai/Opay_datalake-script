# -*- coding: UTF-8 -*-
import requests
import json
import time
import os


class ComwxApi(object):

    #获取访问接口token
    comwx_token_api = 'https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid={0}&corpsecret={1}'

    #发送应用消息接口
    comwx_appmsg_api = 'https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={}'

    #创建群聊接口
    comwx_createchat_api = 'https://qyapi.weixin.qq.com/cgi-bin/appchat/create?access_token={}'

    #修改群
    comwx_editchat_api = 'https://qyapi.weixin.qq.com/cgi-bin/appchat/update?access_token={}'

    #发送群聊消息
    comwx_postchat_api = 'https://qyapi.weixin.qq.com/cgi-bin/appchat/send?access_token={}'

    #存储临时token文件
    token_file = '/tmp/token'

    """
    AgentId 1000011
    初始化对象
    @:param corpid 企业ID wwd26d45f97ea74ad2
    @:param corpsecret 对应应用的secret  BLE_v25zCmnZaFUgum93j3zVBDK-DjtRkLisI_Wns4g
    """
    def __init__(self, corpid, corpsecret, agentid):
        self.corpid = corpid
        self.corpsecret = corpsecret
        self.agentid = agentid

    """
    获取企业微信访问接口的accesstoken
    @:return string
    """
    def getAccessToken(self):
        try:
            if os.path.isfile(self.token_file):
                with open(self.token_file, 'r') as f:
                    context = f.read()
                    if context.strip() != '':
                        token, tm, exp = context.strip().split('|')
                        if float(tm) + int(exp) > time.time():
                            return token

            #get from wx
            uri = self.comwx_token_api.format(self.corpid, self.corpsecret)
            response = requests.get(uri)
            if response.raise_for_status():
                raise ValueError('failed')
            result = response.json()
            if int(result.get('errcode', -1)) == 0:
                #保存token到file/redis
                with open(self.token_file, 'w') as f:
                    f.write('{0}|{1}|{2}'.format(result.get('access_token'), str(time.time()), result.get('expires_in')))
                return result.get('access_token')
            else:
                return None
        except BaseException as e:
            print(e)
            return None

    """
    发送应用消息到用户列表/部门列表
    @:param msg 文本消息内容
    @:param toparty 部门列表 | 分隔
    @:param touser 用户列表 | 分隔
    @:return bool
    """
    def postAppMessage(self, msg, toparty='', touser=''):
        try:
            params = {}
            if touser != '':
                params['touser'] = touser
            if toparty != '':
                params['toparty'] = toparty
            params['msgtype'] = "text"
            params['agentid'] = self.agentid
            params['text'] = {'content': msg}
            params['safe'] = 0

            headers = {'Content-Type': 'application/json'}

            retry = 0
            while True:
                if retry >= 5:
                    return False
                token = self.getAccessToken()
                uri = self.comwx_appmsg_api.format(token)
                response = requests.post(url=uri, headers=headers, data=json.dumps(params))
                if response.raise_for_status():
                    raise ValueError('failed')
                result = response.json()
                if int(result.get('errcode', -1)) == 0:
                    return True
                elif int(result.get('errcode', -1)) == 42001 or int(result.get('errcode', -1)) == 40014:
                    retry += 1
                    time.sleep(0.2)
                    continue
                else:
                    return False

            return False
        except BaseException as e:
            print(e)
            return False

    """
    创建群聊
    @:param users ,分隔的用户ID
    @:param name 群聊名称
    @:param chatid 群聊ID
    @:param owner 群主ID
    @:return chatid
    """
    def createChat(self, users, name='', chatid='', owner=''):
        try:
            params = {}
            params['name'] = name
            params['owner'] = owner
            params['userlist'] = [u.strip() for u in users.split(',')]
            params['chatid'] = chatid

            headers = {'Content-Type': 'application/json'}

            retry = 0
            while True:
                if retry >= 5:
                    return None
                token = self.getAccessToken()
                uri = self.comwx_createchat_api.format(token)
                response = requests.post(url=uri, data=json.dumps(params), headers=headers)
                if response.raise_for_status():
                    raise ValueError('failed')
                result = response.json()
                #print(result)
                if int(result.get('errcode', -1)) == 0:
                    return result.chatid
                elif int(result.get('errcode', -1)) == 42001 or int(result.get('errcode', -1)) == 40014:
                    retry += 1
                    time.sleep(0.2)
                    continue
                else:
                    return None

            return None
        except BaseException as e:
            print(e)
            return None

    """
    更新群聊
    @:param chatid  群聊ID
    @:param addusers 添加成员
    @:param delusers 踢出成员
    @:param owner 群主
    @:param name 群名
    @:return bool
    """
    def updateChat(self, chatid, addusers='', delusers='', owner='', name=''):
        try:
            params = {}
            params['chatid'] = chatid
            params['add_user_list'] = [u.strip() for u in addusers.split(',')]
            params['del_user_list'] = [u.strip() for u in delusers.split(',')]
            if owner != '':
                params['owner'] = owner
            if name != '':
                params['name'] = name

            headers = {'Content-Type': 'application/json'}

            retry = 0
            while True:
                if retry >= 5:
                    return False
                token = self.getAccessToken()
                uri = self.comwx_editchat_api.format(token)
                response = requests.post(url=uri, data=json.dumps(params), headers=headers)
                if response.raise_for_status():
                    raise ValueError('failed')
                result = response.json()
                if int(result.get('errcode', -1)) == 0:
                    return True
                elif int(result.get('errcode', -1)) == 42001 or int(result.get('errcode', -1)) == 40014:
                    retry += 1
                    time.sleep(0.2)
                    continue
                else:
                    return False

            return False
        except BaseException as e:
            print(e)
            return False

    """
    发送群聊文本消息
    @:param chatid 群聊ID
    @:param msg 文本消息
    @:return bool
    """
    def postChatMessage(self, chatid, msg):
        try:
            params = {}
            params['chatid'] = chatid
            params['msgtype'] = 'text'
            params['text'] = {'content': msg}
            params['safe'] = 1

            headers = {'Content-Type': 'application/json'}

            retry = 0
            while True:
                if retry >= 5:
                    return False
                token = self.getAccessToken()
                uri = self.comwx_postchat_api.format(token)
                response = requests.post(url=uri, data=json.dumps(params), headers=headers)
                if response.raise_for_status():
                    raise ValueError('failed')
                result = response.json()
                if int(result.get('errcode', -1)) == 0:
                    return True
                elif int(result.get('errcode', -1)) == 42001 or int(result.get('errcode', -1)) == 40014:
                    retry += 1
                    time.sleep(0.2)
                    continue
                else:
                    return False

            return False
        except BaseException as e:
            print(e)
            return False
