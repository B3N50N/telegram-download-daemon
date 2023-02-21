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
worker_count = 6
updateFrequency = 10
lastUpdate = 0
#multiprocessing.Value('f', 0)


time_a = time.time()
job_c = 0
job_b = 0


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
        #time.sleep(1)


with TelegramClient(getSession(), api_id, api_hash,
                    proxy=proxy).start() as client:

    saveSession(client.session)

    queue = asyncio.Queue()
    
    
    peerChannel = PeerChannel(channel_id)

    @client.on(events.NewMessage())
    async def handler(event):
        global job_c
        global time_a
        global error_count
        
        while (time.time()-time_a)<0.5:
            idle_i =1
        time_a = time.time()
        #while job_c >=6:
        #    time.sleep(5)
        #job_c+=1
        
        #time.sleep(random.randint(0,2))
        

        if event.to_id != peerChannel:
            return

        #print("recieve Msg ID: "+str(event.message.id))
        #job_c = job_c - 1
        
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
                        time.sleep(1)
                        E_rate = psutil.net_io_counters().bytes_recv
                        net_rate = E_rate -S_rate
                        net_rate = net_rate/1024./1024.
                        
                        if output: 
                            output = "Active downloads:  "+ str(len(in_progress)) +"\n\n" + output + "\n------------------------------------------------------------\nWaiting : " + str(queue.qsize())+ "   Fail: "+str(error_count)+"\nSpeed:  "+ str(net_rate)[0:5]+"Mb/s"
                        else: 
                            output = "No active downloads" + "\n------------------------------------------------------------\nWaiting : " + str(queue.qsize())+ "   Fail: "+str(error_count)+"\nSpeed:  "+ str(net_rate)[0:5]+"Mb/s"
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
                        message= await send_reply(event,"{0} added to queue".format(filename))
                        #await msg_queue.put([0,event, "{0} added to queue".format(filename)])
                        await queue.put([event, message])
                else:
                    #message= await send_reply(event,"That is not downloadable. Try to send it as a file.")
                    await msg_queue.put([0,event, "That is not downloadable. Try to send it as a file."])

        except Exception as e:
                print('Events handler error: ', e)
                
        

    async def worker():
        global error_count
        
        while True:
            p_ready =True
            new_event = []
            
            if p_ready:
                try:
                    element = await queue.get()
                    event=element[0]
                    message=element[1]
                    new_event = event
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
                        
                        filename=getFilename(event)
                        fileName, fileExtension = os.path.splitext(filename)
                        tempfilename=fileName+"-"+getRandomId(8)+fileExtension
                        while path.exists("{0}/{1}.{2}".format(tempFolder,tempfilename,TELEGRAM_DAEMON_TEMP_SUFFIX)) or path.exists("{0}/{1}".format(downloadFolder,tempfilename)):
                            tempfilename=fileName+"-"+getRandomId(8)+fileExtension
                            
                        if path.exists("{0}/{1}.{2}".format(tempFolder,tempfilename,TELEGRAM_DAEMON_TEMP_SUFFIX)) or path.exists("{0}/{1}".format(downloadFolder,filename)):
                            if duplicates == "rename":
                                filename=tempfilename

         
                        if hasattr(event.media, 'photo'):
                            size = 0
                        else: 
                            size=event.media.document.size

                        #await log_reply(message,"Downloading file {0} ({1} bytes)".format(filename,size))
                        await msg_queue.put([1,message, "Downloading file {0} ({1} bytes)".format(filename,size)])

                        download_callback = lambda received, total: set_progress(filename, message, received, total)
                        
                        #await client.download_media(event.message, "{0}/{1}.{2}".format(tempFolder,filename,TELEGRAM_DAEMON_TEMP_SUFFIX), progress_callback = download_callback)
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
                        time.sleep(2)
                        #await client.delete_messages(channel_id, message.id)
                        await msg_queue.put([2,channel_id, message.id])
                    except Exception as e:
                        #remove frome status
                        try:
                            in_progress.pop(filename)
                            await msg_queue.put([1,message, "Error: {}".format(str(e)+"\n Retrying......")])
                        except:pass
                        
                        if "No such file or directory:" in str(e) and retry_count<3:
                            retry_flag = True
                            retry_count+=1
                        elif "Timeout while fetching data (caused by GetFileRequest)" in str(e) and retry_count<3:
                            time.sleep(16)
                            retry_flag = True
                            retry_count+=1
                        elif "The file reference has expired" in str(e) and retry_count<3:
                            try:
                                # client.get_messages(channel_id, event.message.id)
                                
                                #await client.forward_messages(channel_id,event.message.id,channel_id)
                                #await msg_queue.put([3,channel_id,event.message.id,channel_id])
                                #await msg_queue.put([2,channel_id, event.message.id])
                                
                                new_event = await client.get_messages(channel_id, ids=event.message.id)
                                
                                retry_flag = True
                                retry_count+=1
                                
                                time.sleep(5)
                                # error_count +=1
                            except Exception as e2:
                                try: 
                                    #await log_reply(message, "Error: {}".format(str(e))) # If it failed, inform the user about it.
                                    await msg_queue.put([1,message, "Error: {}".format(str(e)+"\n"+str(e2))])
                                except: pass
                                print('Queue worker error: ', e)
                                error_count +=1
                            
                            
                        else:
                            try: 
                                #await log_reply(message, "Error: {}".format(str(e))) # If it failed, inform the user about it.
                                await msg_queue.put([1,message, "Error: {}".format(str(e))])
                            except: pass
                            print('Queue worker error: ', e)
                            error_count +=1
                
    async def msg_worker():
        while 1==1:
            try:
                msg_element = await msg_queue.get()
                if msg_element[0] == 0:
                    message= await send_reply(msg_element[1],msg_element[2])
                elif msg_element[0] == 1:
                    await log_reply(msg_element[1], msg_element[2])
                elif msg_element[0] == 2:
                    await client.delete_messages(msg_element[1], msg_element[2])
                elif  msg_element[0] == 3:
                    await client.forward_messages(msg_element[1], msg_element[2], msg_element[3])
                # msg_queue.task_done
            except Exception as e:
                print('message transfer error: ', e)
                print(msg_element)
                
                
            time.sleep(1)
 
    async def start():

        tasks = []
        loop = asyncio.get_event_loop()
        for i in range(worker_count):
            task = loop.create_task(worker())
            tasks.append(task)
        task = loop.create_task(msg_worker())
        tasks.append(task)
        await sendHelloMessage(client, peerChannel)
        await client.run_until_disconnected()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    client.loop.run_until_complete(start())
