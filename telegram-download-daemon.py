#!/usr/bin/env python3
# Telegram Download Daemon
# Author(original code): Alfonso E.M. <alfonso@el-magnifico.org>
# Author(improve stability): 3V1LC47 <b900613@gmail.com>
# You need to install telethon (and cryptg to speed up downloads)

from os import getenv, path
from shutil import move
from retry import retry
import subprocess
import math
import time
import random
import string
import os.path
from mimetypes import guess_extension

from sessionManager import getSession, saveSession

from telethon import TelegramClient, events, __version__
from telethon.tl.types import PeerChannel, DocumentAttributeFilename, DocumentAttributeVideo
import logging

logging.basicConfig(format='[%(levelname) 5s/%(asctime)s]%(name)s:%(message)s',
                    level=logging.WARNING)

import multiprocessing
import argparse
import asyncio
import numpy
import psutil


TDD_VERSION="1.13-3V1LC4T"

TELEGRAM_DAEMON_API_ID = getenv("TELEGRAM_DAEMON_API_ID")
TELEGRAM_DAEMON_API_HASH = getenv("TELEGRAM_DAEMON_API_HASH")
TELEGRAM_DAEMON_CHANNEL = getenv("TELEGRAM_DAEMON_CHANNEL")

TELEGRAM_DAEMON_SESSION_PATH = getenv("TELEGRAM_DAEMON_SESSION_PATH")

TELEGRAM_DAEMON_DEST=getenv("TELEGRAM_DAEMON_DEST", "/telegram-downloads")
TELEGRAM_DAEMON_TEMP=getenv("TELEGRAM_DAEMON_TEMP", "")
TELEGRAM_DAEMON_DUPLICATES=getenv("TELEGRAM_DAEMON_DUPLICATES", "rename")

TELEGRAM_DAEMON_TEMP_SUFFIX="tdd"

parser = argparse.ArgumentParser(
    description="Script to download files from a Telegram Channel.")
parser.add_argument(
    "--api-id",
    required=TELEGRAM_DAEMON_API_ID == None,
    type=int,
    default=TELEGRAM_DAEMON_API_ID,
    help=
    'api_id from https://core.telegram.org/api/obtaining_api_id (default is TELEGRAM_DAEMON_API_ID env var)'
)
parser.add_argument(
    "--api-hash",
    required=TELEGRAM_DAEMON_API_HASH == None,
    type=str,
    default=TELEGRAM_DAEMON_API_HASH,
    help=
    'api_hash from https://core.telegram.org/api/obtaining_api_id (default is TELEGRAM_DAEMON_API_HASH env var)'
)
parser.add_argument(
    "--dest",
    type=str,
    default=TELEGRAM_DAEMON_DEST,
    help=
    'Destination path for downloaded files (default is /telegram-downloads).')
parser.add_argument(
    "--temp",
    type=str,
    default=TELEGRAM_DAEMON_TEMP,
    help=
    'Destination path for temporary files (default is using the same downloaded files directory).')
parser.add_argument(
    "--channel",
    required=TELEGRAM_DAEMON_CHANNEL == None,
    type=int,
    default=TELEGRAM_DAEMON_CHANNEL,
    help=
    'Channel id to download from it (default is TELEGRAM_DAEMON_CHANNEL env var'
)
parser.add_argument(
    "--duplicates",
    choices=["ignore", "rename", "overwrite"],
    type=str,
    default=TELEGRAM_DAEMON_DUPLICATES,
    help=
    '"ignore"=do not download duplicated files, "rename"=add a random suffix, "overwrite"=redownload and overwrite.'
)
args = parser.parse_args()

api_id = args.api_id
api_hash = args.api_hash
channel_id = args.channel
downloadFolder = args.dest
tempFolder = args.temp
duplicates=args.duplicates
worker_count = multiprocessing.cpu_count()
worker_count = 8
updateFrequency = 10
lastUpdate = 0
MSG_trans_retry_delay = 8
MSG_trans_retry_limit = 3
#multiprocessing.Value('f', 0)


time_a = time.time()


if not tempFolder:
    tempFolder = downloadFolder

# Edit these lines:
proxy = None

# End of interesting parameters

async def sendHelloMessage(client, peerChannel):
    entity = await client.get_entity(peerChannel)
    print("Telegram Download Daemon "+TDD_VERSION+" using Telethon "+__version__)
    await client.send_message(entity, "Telegram Download Daemon "+TDD_VERSION+" using Telethon "+__version__)
    await client.send_message(entity, "Hi! Ready for your files!")
 
@retry(tries=10,delay=8)
async def log_reply(message, reply):
    #print(reply)
    await message.edit(reply)
    
@retry(tries=10,delay=8)
async def send_reply(event,msg):
    
    message=await event.reply(msg)
    return message

def getRandomId(len):
    chars=string.ascii_lowercase + string.digits
    return  ''.join(random.choice(chars) for x in range(len))
 
def getFilename(event: events.NewMessage.Event):
    mediaFileName = "unknown"

    if hasattr(event.media, 'photo'):
        mediaFileName = str(event.media.photo.id)+".jpeg"
    elif hasattr(event.media, 'document'):
        for attribute in event.media.document.attributes:
            if isinstance(attribute, DocumentAttributeFilename): 
              mediaFileName=attribute.file_name
              break     
            if isinstance(attribute, DocumentAttributeVideo):
              if event.original_update.message.message != '': 
                  mediaFileName = event.original_update.message.message
              else:    
                  mediaFileName = str(event.message.media.document.id)
              mediaFileName+=guess_extension(event.message.media.document.mime_type)    
     
    mediaFileName="".join(c for c in mediaFileName if c.isalnum() or c in "()._- ")
      
    return mediaFileName

status_msg = ""
status_update_time = time.time()
in_progress={}
msg_queue = asyncio.Queue()
error_count = 0

async def set_progress(filename, message, received, total):
    global lastUpdate
    global updateFrequency
    global msg_queue

    if received >= total:
        try: in_progress.pop(filename)
        except: pass
        return
    percentage = math.trunc(received / total * 10000) / 100

    progress_message= "{0} % ({1} / {2})".format(percentage, received, total)
    in_progress[filename] = progress_message

    currentTime=time.time()
    if (currentTime - lastUpdate) > updateFrequency:
        #await log_reply(message, progress_message)
        await msg_queue.put([1,message, progress_message])
        lastUpdate=currentTime
        #await asyncio.sleep(1)


with TelegramClient(getSession(), api_id, api_hash,
                    proxy=proxy).start() as client:

    saveSession(client.session)

    queue = asyncio.Queue()
    missing_msg_queue = asyncio.Queue()
    last_msg_id = 0
    
    
    peerChannel = PeerChannel(channel_id)
    

    @client.on(events.NewMessage())
    async def handler(event):
        global time_a
        global error_count
        global status_msg
        global last_msg_id
        
        while (time.time()-time_a)<0.2:
            idle_i =1
        time_a = time.time()
        

        if event.to_id != peerChannel:
            return
            
        if last_msg_id != 0 and int(event.message.id) != (last_msg_id+1) and int(event.message.id) > last_msg_id:
            for i in range(last_msg_id+1,int(event.message.id)):
                await missing_msg_queue.put(i)
        last_msg_id = int(event.message.id)

        # print("recieve Msg ID: "+str(event.message.id))
        
        try:

            if (not event.media) and event.message:
                command = event.message.message
                command = command.lower()
                output = "Unknown command"
                
                if_send =False

                if command == "list":
                    try:
                        output = subprocess.run(["ls -l "+downloadFolder], shell=True, stdout=subprocess.PIPE,stderr=subprocess.STDOUT).stdout.decode('utf-8')
                    except:
                        output = "list too long to display"
                        
                    if_send = True
                    
                elif command == "status":
                    try:
                        
                        
                        output = "".join([ "{0}: {1}\n".format(key,value) for (key, value) in in_progress.items()])
                        
                        #net speed
                        S_rate = psutil.net_io_counters().bytes_recv
                        await asyncio.sleep(1)
                        E_rate = psutil.net_io_counters().bytes_recv
                        net_rate = E_rate -S_rate
                        net_rate = net_rate/1024./1024.
                        
                        if output: 
                            output = "Active downloads:  "+ str(len(in_progress)) +"\n\n" + output + "\n------------------------------------------------------------\nWaiting : " + str(queue.qsize())+ "   Fail: "+str(error_count)+"\nSpeed:  "+ str(net_rate)[0:5]+"Mb/s"
                        else: 
                            output = "No active downloads" + "\n------------------------------------------------------------\nWaiting : " + str(queue.qsize())+ "   Fail: "+str(error_count)
                        
                        #remove old status message
                        try:
                            status_update_time = time.time()
                            await msg_queue.put([2,channel_id, status_msg])    
                            status_msg = event.message.id
                        except:pass
                        
                    except Exception as e:
                        output = "Some error occured while checking the status. Retry.("+str(e)+")"
                    if_send = True
                elif command == "clean":
                    output = "Cleaning "+tempFolder+"\n"
                    output+=subprocess.run(["rm "+tempFolder+"/*."+TELEGRAM_DAEMON_TEMP_SUFFIX], shell=True, stdout=subprocess.PIPE,stderr=subprocess.STDOUT).stdout
                    error_count = 0
                    
                    if_send = True
                elif command == "current":
                    pass
                elif command == "checkhis":
                    await pre_forward()
                elif "added to queue" in command:
                    output = event.message.message
                
                else:
                    #print("MSG: Command -> " + str(command) + " is not Available")
                    output = "MSG: Command -> " + str(command) + " is not Available"+"\n"+"Available commands: list, status, clean"
                    if_send = True
                
                if if_send :
                    #await log_reply(event, output)
                    await msg_queue.put([1,event, output])

            if event.media:
                if hasattr(event.media, 'document') or hasattr(event.media,'photo'):
                    filename=getFilename(event)
                    if ( path.exists("{0}/{1}.{2}".format(tempFolder,filename,TELEGRAM_DAEMON_TEMP_SUFFIX)) or path.exists("{0}/{1}".format(downloadFolder,filename)) ) and duplicates == "ignore":
                        #message= await send_reply(event,"{0} already exists. Ignoring it.".format(filename))
                        await msg_queue.put([0,event, "{0} already exists. Ignoring it.".format(filename)])
                    else:
                        #message= await send_reply(event,"{0} added to queue".format(filename))
                        #await queue.put([event, message])
                        await msg_queue.put([4,event, "{0} added to queue".format(filename)],)
                else:
                    #message= await send_reply(event,"That is not downloadable. Try to send it as a file.")
                    await msg_queue.put([0,event, "That is not downloadable. Try to send it as a file."])

        except Exception as e:
                print('Events handler error: ', e)
                
        

    async def worker():
        global error_count
        global status_update_time
        
        while True:
            p_ready =True
            new_event = []
            
            if p_ready:
                try:
                    element = await queue.get()
                    event=element[0]
                    message=element[1]
                    new_event = event
                    
                    filename=getFilename(event)
                    if len(filename)>20:
                        filename =str(event.media.document.id) + os.path.splitext(getFilename(event))[1]
                        
                    fileName, fileExtension = os.path.splitext(filename)
                    tempfilename=fileName+"-"+getRandomId(8)+fileExtension
                    while path.exists("{0}/{1}.{2}".format(tempFolder,tempfilename,TELEGRAM_DAEMON_TEMP_SUFFIX)) or path.exists("{0}/{1}".format(downloadFolder,tempfilename)):
                        tempfilename=fileName+"-"+getRandomId(8)+fileExtension
                        
                    if path.exists("{0}/{1}.{2}".format(tempFolder,tempfilename,TELEGRAM_DAEMON_TEMP_SUFFIX)) or path.exists("{0}/{1}".format(downloadFolder,filename)):
                        if duplicates == "rename":
                            filename=tempfilename
                            
                except Exception as e:
                    p_ready =False
                    try: 
                        #await log_reply(message, "Error: {}".format(str(e))) # If it failed, inform the user about it.
                        await msg_queue.put([1,message, "Error: {}".format(str(e))])
                    except: pass
                    print('Queue worker error: ', e)
            if p_ready:
                retry_flag = True
                retry_count = 0
                while retry_flag == True:            
                    try:
                        retry_flag = False
         
                        if hasattr(event.media, 'photo'):
                            size = 0
                        else: 
                            size=event.media.document.size

                        #await log_reply(message,"Downloading file {0} ({1} bytes)".format(filename,size))
                        await msg_queue.put([1,message, "Downloading file {0} ({1} bytes)".format(filename,size)])

                        download_callback = lambda received, total: set_progress(filename, message, received, total)
                        
                        #await client.download_media(new_event.message, "{0}/{1}.{2}".format(tempFolder,filename,TELEGRAM_DAEMON_TEMP_SUFFIX), progress_callback = download_callback)
                        await client.download_media(new_event.media, "{0}/{1}.{2}".format(tempFolder,filename,TELEGRAM_DAEMON_TEMP_SUFFIX), progress_callback = download_callback)
                        #await client.download_media(str(event.media.document.id), "{0}/{1}.{2}".format(tempFolder,filename,TELEGRAM_DAEMON_TEMP_SUFFIX), progress_callback = download_callback)
                        
                        
                        set_progress(filename, message, 100, 100)
                        move("{0}/{1}.{2}".format(tempFolder,filename,TELEGRAM_DAEMON_TEMP_SUFFIX), "{0}/{1}".format(downloadFolder,filename))
                        #await log_reply(message, "{0} ready".format(filename))
                        await msg_queue.put([1,message, "{0} ready".format(filename)])
                    
                        queue.task_done()
                        #delete message
                        #await client.delete_messages(channel_id, event.message.id)
                        await msg_queue.put([2,channel_id, event.message.id])
                        await asyncio.sleep(2)
                        #await client.delete_messages(channel_id, message.id)
                        await msg_queue.put([2,channel_id, message.id])
                        if time.time() - status_update_time >=30 or len(in_progress) == 0:
                            await msg_queue.put([0,event, "status"])
                            status_update_time = time.time()
                        else:
                            pass
                    except Exception as e:
                        #remove frome status
                        try:
                            in_progress.pop(filename)
                            await msg_queue.put([1,message, "Error: {}".format(str(e)+"\n Retrying......")])
                        except:pass
                        
                        if "File name too long" in str(e):
                            filename=str(event.media.document.id) + os.path.splitext(getFilename(event))[1]
                            fileName, fileExtension = os.path.splitext(filename)
                            tempfilename=fileName+"-"+getRandomId(8)+fileExtension
                            while path.exists("{0}/{1}.{2}".format(tempFolder,tempfilename,TELEGRAM_DAEMON_TEMP_SUFFIX)) or path.exists("{0}/{1}".format(downloadFolder,tempfilename)):
                                tempfilename=fileName+"-"+getRandomId(8)+fileExtension
                                
                            if path.exists("{0}/{1}.{2}".format(tempFolder,tempfilename,TELEGRAM_DAEMON_TEMP_SUFFIX)) or path.exists("{0}/{1}".format(downloadFolder,filename)):
                                if duplicates == "rename":
                                    filename=tempfilename
                                    
                            retry_flag = True
                        elif "No such file or directory:" in str(e) and retry_count<=MSG_trans_retry_limit:
                            retry_flag = True
                            retry_count+=1
                        elif "Timeout while fetching data (caused by GetFileRequest)" in str(e) and retry_count<=MSG_trans_retry_limit:
                            await asyncio.sleep(MSG_trans_retry_delay*retry_count)
                            retry_flag = True
                            retry_count+=1
                        elif "The file reference has expired" in str(e) and retry_count<=MSG_trans_retry_limit:
                            try:
                                # client.get_messages(channel_id, event.message.id)
                                
                                #await client.forward_messages(channel_id,event.message.id,channel_id)
                                #await msg_queue.put([3,channel_id,event.message.id,channel_id])
                                #await msg_queue.put([2,channel_id, event.message.id])
                                
                                new_event = await client.get_messages(channel_id, ids=event.message.id)
                                
                                retry_flag = True
                                retry_count+=1
                                
                                await asyncio.sleep(MSG_trans_retry_delay)
                                # error_count +=1
                            except Exception as e2:
                                try:
                                    #await log_reply(message, "Error: {}".format(str(e))) # If it failed, inform the user about it.
                                    #await msg_queue.put([1,message, "Error: {}".format(str(e)+"\n"+str(e2))])
                                    await msg_queue.put([1,message, "Error: {}".format(str(e)+"\n"+str(e2)+"\nDowload canceled or media not exist")])
                                except: pass
                                print('Queue worker error: ', e)
                                error_count +=1
                         
                        elif "'NoneType' object has no attribute 'media'" in str(e):
                            try:
                                await msg_queue.put([2,channel_id, message.id])
                            except: pass
                            
                            
                        else:
                            try: 
                                #await log_reply(message, "Error: {}".format(str(e))) # If it failed, inform the user about it.
                                await msg_queue.put([1,message, "Error: {}".format(str(e))])
                            except: pass
                            print('Queue worker error: ', e)
                            error_count +=1
                
    async def msg_transfer_worker():
        global MSG_trans_retry_delay
        global MSG_trans_retry_limit
        
        while 1==1:
            retry_flag = True
            retry_count = 0
            while retry_flag:
                try:
                    retry_flag = False
                    msg_element = await msg_queue.get()
                    if msg_element[0] == 0:
                        message = await send_reply(msg_element[1],msg_element[2])
                    elif msg_element[0] == 1:
                        await log_reply(msg_element[1], msg_element[2])
                    elif msg_element[0] == 2:
                        await client.delete_messages(msg_element[1], msg_element[2])
                    elif  msg_element[0] == 3:
                        await client.forward_messages(msg_element[1], msg_element[2], msg_element[3])
                    elif  msg_element[0] == 4:
                        message= await send_reply(msg_element[1],msg_element[2])
                        await queue.put([msg_element[1], message])
                    elif  msg_element[0] == 5:
                        await client.forward_messages(msg_element[1], msg_element[2], msg_element[3])
                        await client.delete_messages(msg_element[1],msg_element[2])
                    # msg_queue.task_done
                except Exception as e:
                    # print('retransfer: ', str(e))
                    if "Content of the message was not modified (caused by EditMessageRequest)" in str(e) and retry_count<=MSG_trans_retry_limit:
                        retry_flag = True
                        retry_count+=1
                        await asyncio.sleep(MSG_trans_retry_delay*retry_count)
                    elif "A wait of" in str(e) and "seconds is required" in str(e):
                        print('message transfer retry: ', e)
                        await asyncio.sleep(int(str(e).split(" ")[3]))
                        retry_flag = True
                    elif "The specified message ID is invalid or you can't do that operation on such message" in str(e):
                        pass
                    elif (msg_element[0] == 4 or msg_element[0] == 5) and retry_count<=MSG_trans_retry_limit:
                        retry_flag = True
                        await asyncio.sleep(MSG_trans_retry_delay)
                    else:
                        print("message transfer error"+str(msg_element[0])+": "+ str(e))
                        print(msg_element)
                
            await asyncio.sleep(0.3)
            
    async def msg_fill_worker():
        
        while 1==1:
            try:
                missing_id = await missing_msg_queue.get()
                await msg_queue.put([3,channel_id,missing_id,channel_id])
                await msg_queue.put([2,channel_id, missing_id])
            except Exception as e:
                print("message resend error: ("+ str(missing_id) +")"+str(e) )
                
            
            
    async def pre_forward():
        history_msg = []
        print("start reading")
        async for message in client.iter_messages(channel_id, reverse= True):
            history_msg.append(message.id)
        
        print("finish reading message count: "+ str(len(history_msg)))
        
        for msg_id in history_msg:
            message = await client.get_messages(channel_id, ids=msg_id)
            if message.media :
                #await msg_queue.put([3,channel_id, msg_id, channel_id])
                #await msg_queue.put([2,channel_id, msg_id])
                await msg_queue.put([5,channel_id, msg_id, channel_id])
            else:
                await msg_queue.put([2,channel_id, msg_id])
            await asyncio.sleep(1)
            
        print("finish pre forward")
 
    async def start():

        tasks = []
        loop = asyncio.get_event_loop()
        for i in range(worker_count):
            task = loop.create_task(worker())
            tasks.append(task)
        task = loop.create_task(msg_transfer_worker())
        tasks.append(task)
        task = loop.create_task(msg_fill_worker())
        tasks.append(task)
        await sendHelloMessage(client, peerChannel)
        await pre_forward()
        await client.run_until_disconnected()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    client.loop.run_until_complete(start())
